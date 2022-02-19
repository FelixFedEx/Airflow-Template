from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email_operator import EmailOperator

from subdags.data_pipeline_definition import CustomEail, Config
from subdags.data_pipeline_common_utils import save_to_s3

from boto3 import resource, client
from boto3.dynamodb.conditions import Key, Attr
import pandas as pd

from tempfile import NamedTemporaryFile
from datetime import datetime, timedelta
now = datetime.now()
yesterday = now - timedelta(days = 1)


REPORT_S3_BUCKET_NAME = Config.REPORT_S3_BUCKET_NAME
DAILY_REPORT_PATH = Config.DAILY_REPORT_PATH
PLANET = CustomEail.PLANET
DYNAMODB_TABLE_NAME = Config.DYNAMODB_TABLE_NAME
STAKEHOLDER_EMAIL = CustomEail.STAKEHOLDER_EMAIL
EMAIL = CustomEail.ERROR_MAIL 

HOUR = Config.REPORT_START_HOUR  # 19
MINUTE = Config.REPORT_START_MINUTE # 30
# for daily report (dev)
START_DATE = datetime(yesterday.year, yesterday.month, yesterday.day, HOUR, MINUTE).strftime("%Y-%m-%d %H:%M:%S")
END_DATE = datetime(now.year, now.month, now.day, HOUR, MINUTE).strftime("%Y-%m-%d %H:%M:%S")



def generate_dynamodb_filter(filter_: dict):
    tmp_filter = []
    for f in filter_:
        print(f'Append: {f} - {filter_.get(f)}')
        if f in ['created_at', 'updated_at']:
            if filter_.get(f):
                tmp_filter.append(Attr(f).between(filter_.get(f)[0], filter_.get(f)[1]))
        else:
            tmp_filter.append(Attr(f).eq(filter_.get(f)))

    dynamo_filter = tmp_filter[0]
    for f in tmp_filter[1:]:
        dynamo_filter &= f
        
    return dynamo_filter


def query_dynamodb(table_name: str, filter_) -> pd.DataFrame:
    dynamodb = resource('dynamodb')
    table = dynamodb.Table(table_name)
    
    dynamodb_filter = generate_dynamodb_filter(filter_)
    response = table.scan(FilterExpression = dynamodb_filter)
    
    if len(response['Items']) != 0:
        df = pd.concat([pd.json_normalize(res) for res in response['Items']], ignore_index = True)
        return df[['file_name', 'customer', 'business', 'feature', 'updated_at', 'note', 'processed_status', 'error_code', 'phase', 
                   'delivery_status', 'created_at', 'command_id', 'run_id', 'dag_id']]
    return None


def read_dynamodb(dynamodb_table_name: str, time_range: list, s3_bucket_name: str, file_path: str, **context) -> None:
    df_report = query_dynamodb(dynamodb_table_name, {'updated_at': [time_range[0], time_range[1]]})
    file_name = file_path + f'Report_{time_range[1][:10]}.csv'
    if df_report is not None:
        save_to_s3(dataframe = df_report, bucket_name = s3_bucket_name, file_path = file_name)
        context['task_instance'].xcom_push(key = 'report_name', value = file_name)
        print(f'Save to {s3_bucket_name}/{file_path} successfully.')
        return 'dataset_exists'
    else:
        return 'no_dataset_exists'


def send_email(s3_bucket_name: str, owner_type: str, business: str, customer: list, email: list, planet: str, 
               time_stamp: str, index: int, **context) -> None:
    
    def send_email_operator(df: pd.DataFrame, owner_type: str, receiver: str, index: int, business: str, customer: list, email: list, 
                            planet: str) -> None:
        with NamedTemporaryFile(mode='w+', suffix=".csv", prefix = f'Daily_Failure_Report_{time_stamp}_') as file:
            df.to_csv(file.name, index = False)
            attachment = [file.name] if df.shape[0]!=0 else []
            number_of_failure =  df[(df.processed_status.isin(['failed']))|(df.note=='invalid file name')].shape[0]
 
            send_eamil_operator = EmailOperator(
                task_id = f"send_eamil_to_{owner_type}_{index}", 
                to = email,
                bcc = CustomEail.ERROR_MAIL,
                subject = f"Daily Failure Report {time_stamp}",
                html_content = f'Hi {receiver} User, <br><br>'
                                f"Number of files failed to process: {number_of_failure}. <br>"
                                'Please find details in the attachment. <br>'
                                'Thank you. <br><br>',
                files = attachment,
            )

            send_eamil_operator.execute(context)
    
    report_name = context['task_instance'].xcom_pull(key = 'report_name')
    business_ = business[0].upper() + business[1:]

    s3 = client('s3')
    s3_object = s3.get_object(Bucket = s3_bucket_name, Key = report_name)
    df = pd.read_csv(s3_object['Body'])

    # Check if customer uploaded data during the period. If not, return directly.
    if (not df.customer.isin(customer).sum()) and (owner_type != 'business'):
        print("customer did not upload any data.")
        return

    send_email_operator(df, owner_type, 'business', index, business_, customer, email, planet)


def group_contact_list(contact_list: dict) -> tuple:
    array_df = []
    for business in contact_list:
        for customer in contact_list[business]:
            # print(var[business][customer])
            array_df.append([business, customer, contact_list[business][customer]['us_email'], 
                             contact_list[business][customer]['customer_email']])
    df_contact = pd.DataFrame(array_df, columns = ['business', 'customer', 'us_email', 'customer_email'])
    
    us_contact = df_contact.groupby([df_contact.business, df_contact.us_email.map(tuple)]).customer.apply(list).reset_index()
    us_contact['us_email'] = us_contact.us_email.map(list)
    us_contact.rename({'us_email': 'email'}, axis = 1, inplace = True)
    
    customer_contact = df_contact.groupby([df_contact.business, df_contact.customer_email.map(tuple)]).customer.apply(list).reset_index()
    customer_contact['customer_email'] = customer_contact.customer_email.map(list)
    customer_contact.rename({'customer_email': 'email'}, axis = 1, inplace = True)
    
    return us_contact, customer_contact


default_args = {
    'owner': 'felix',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2022, 1, 1)
}


with DAG(dag_id = 'report', 
        schedule_interval = '30 11 * * *', # UTC 11:30 = Taipei Time 19:30
        default_args = default_args, 
        catchup=False,
        ) as dag:

    get_report = BranchPythonOperator(
        task_id = 'get_report',
        python_callable = read_dynamodb,
        op_kwargs = {
            'dynamodb_table_name': DYNAMODB_TABLE_NAME, 
            'time_range': [START_DATE, END_DATE], 
            's3_bucket_name': REPORT_S3_BUCKET_NAME, 
            'file_path': DAILY_REPORT_PATH
        }        
    )

    dataset_exists = DummyOperator(
        task_id = 'dataset_exists'
    )

    no_dataset_exists = DummyOperator(
        task_id = 'no_dataset_exists'
    )

    us_contact, customer_contact = group_contact_list(STAKEHOLDER_EMAIL)
    
    # supplier
    for ind, row in customer_contact.iterrows():
        print(row['email'], row['customer'])
        send_email_to_supplier = PythonOperator(
            task_id = f"send_email_supplier_{ind}", 
            python_callable = send_email,  
            dag = dag,
            op_kwargs = {
                's3_bucket_name': REPORT_S3_BUCKET_NAME, 
                'owner_type': 'supplier',
                'business': row['business'],
                'customer': row['customer'],
                'email': row['email'],
                'planet': PLANET,
                'time_stamp': END_DATE[:10],
                'index': ind
            }
        )

        dataset_exists >> send_email_to_supplier
    
    for ind, row in us_contact.iterrows():
        print(row['email'], row['customer'])
        send_email_to_po = PythonOperator(
            task_id = f"send_email_po_{ind}", 
            python_callable = send_email,  
            dag = dag,
            op_kwargs = {
                's3_bucket_name': REPORT_S3_BUCKET_NAME, 
                'owner_type': 'us_owner', 
                'business': row['business'],
                'customer': row['customer'],
                'email': row['email'],
                'planet': PLANET,
                'time_stamp': END_DATE[:10],
                'index': ind
            }
        )

        dataset_exists >> send_email_to_po

    send_email_to_felix = PythonOperator(
        task_id = f"send_email_felix", 
        python_callable = send_email,  
        dag = dag,
        op_kwargs = {
            's3_bucket_name': REPORT_S3_BUCKET_NAME, 
            'owner_type': 'business', 
            'business': 'Overall',
            'customer': [None],
            'email': EMAIL,
            'planet': PLANET,
            'time_stamp': END_DATE[:10],
            'index': 0
        }
    )

    get_report >> dataset_exists >> send_email_to_felix
    get_report >> no_dataset_exists

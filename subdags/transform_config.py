from airflow import DAG  
from airflow.operators.dummy_operator import DummyOperator
# from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.utils.task_group import TaskGroup
from subdags.data_pipeline_common_utils import delete_file_from_s3, save_to_s3, check_file_type, dataframe_to_sql, save_from_s3_to_mysql, check_is_xlsx
from subdags.data_pipeline_definition import Config, CustomError

from io import BytesIO
import pandas as pd
import numpy as np
import boto3

def get_sub_category(filename):
    if filename.split('_')[4].lower() == 'p':
        return 'cat-1'
    elif filename.split('_')[4].lower() == 'f':
        return 'cat-2'
    elif filename.split('_')[4].lower() == 'd':
        return 'cat-3'

def get_regularized_column_header(df_):
    list_column = df_.columns.to_list()
    list_column_regularized = [x.lower().replace('/', '').replace(' ', '_') for x in list_column]

    df_.columns = list_column_regularized
    
    return df_

def get_regularized_data(df_,header):
    df_[header] = df_[header].str.strip()
    df_[header] = df_[header].str.upper()
    # CRLF will transform to '_X000D_' on read_excel, so need to remove it.
    df_[header] = df_[header].replace("_X000D_","",regex = True)
    
    return df_
    
def read_data(s3_bucket_name, filepath_data, mysql_conn_id, tablename_code):
    s3 = boto3.client('s3')
    s3_data = s3.get_object(Bucket = s3_bucket_name, Key = filepath_data)
    s3_data_io = BytesIO(s3_data['Body'].read())
    is_xlsx = check_is_xlsx(s3_data_io)
    filetype = check_file_type(filepath_data)
    print(f'is_xlsx: {is_xlsx}')
    
    if (filetype == 'csv') and (not is_xlsx):
        try:
            obj_data = pd.read_csv(s3_data_io, usecols = [1], skiprows = 6).replace('^\s*$', np.nan, regex = True).dropna(how = 'all')
        except:
            raise IOError(f"Error: Failed to read file.")    
    elif (filetype == 'xlsx') and is_xlsx:
        try:
            obj_data = pd.read_excel(s3_data_io, usecols = [1], skiprows = 6).replace('^\s*$', np.nan, regex = True).dropna(how = 'all')
        except:
            raise IOError(f"Error: Failed to read file.")
    else:
        raise IOError(f"Error: Failed to read file. Mismatched file type and name.")


    obj_data.columns = ['label']
    obj_data = get_regularized_data(obj_data,'label')
    
    try:
        hook = MySqlHook(mysql_conn_id)
        obj_code = hook.get_pandas_df(f"""SELECT 
                                                    code, series, generation
                                                FROM {tablename_code}
                                                WHERE code = '{obj_data.label.str[:5].values[0]}';""")
    except:
        raise IOError(f"Error: Failed to read Assembly Code: '{obj_data.label.str[:5].values[0]}'.")
    
    return obj_data, obj_code


def clean_data(obj_data, obj_code, filepath_data) -> pd.DataFrame:
    obj_data['code'] = obj_data.label.str[:5]
    
    # extract vendor code & pcs_date_code from label
    obj_data['vendor'] = obj_data.label.str[8:10]
    obj_data['pcs_date_code'] = obj_data.label.str[10:13]
    
    # Phase are from the file name
    obj_data['phase'] = filepath_data.split('/')[-1].split('_')[3]
    
    # Map date and week from serial number
    obj_data['date'] = obj_data.pcs_date_code.apply(lambda x: pd.Timestamp(Config.MAP_YEAR[x[0]], Config.MAP_MONTH[x[1]], Config.MAP_DAY[x[2]]))
    obj_data['week'] = obj_data.date.dt.week
    
    obj_data = obj_data.merge(obj_code, on = 'code', how = 'left')
    obj_data = obj_data[['label', 'code', 'series', 'generation', 'component', 
                         'vendor', 'phase', 'date', 'week']]
    
    return obj_data


def transform_main(s3_bucket_name: str, filepath_data: str, filepath_processed: str, 
                   mysql_conn_id:str, tablename_code: str, filename = str, **kwargs) -> None:
    #obj_data, obj_code = read_data(s3_bucket_name, filepath_data, filepath_assemly_code)
    obj_data, obj_code = read_data(s3_bucket_name, filepath_data, mysql_conn_id, tablename_code)
    result = clean_data(obj_data, obj_code, filepath_data)
    
    sub_category = get_sub_category(filename)
    filepath_published_quality_suffix = f"""{result.values[0][4]}/{sub_category}/Data/series={result.values[0][2]}/generation={result.values[0][3]}/code={result.values[0][1]}/year={result.values[0][7].year}/week={result.values[0][8]}/"""
    filepath_published_inspect_suffix = f"""{result.values[0][4]}/{sub_category}/Activity/series={result.values[0][2]}/generation={result.values[0][3]}/code={result.values[0][1]}/year={result.values[0][7].year}/week={result.values[0][8]}/"""                                
    
    kwargs['ti'].xcom_push(key = 'filepath_published_quality_suffix', value = filepath_published_quality_suffix)
    kwargs['ti'].xcom_push(key = 'filepath_published_inspect_suffix', value = filepath_published_inspect_suffix)
    kwargs['ti'].xcom_push(key = 'component', value = result.values[0][4])

    save_to_s3(result, s3_bucket_name, filepath_processed)


def save_config_to_mysql(dag: DAG, filename_dict: dict) -> TaskGroup:
    with TaskGroup(group_id='save_config_to_rds_task_group') as save_config_to_rds_task_group:

        create_mysql_table = MySqlOperator(
            task_id = 'create_mysql_table',
            sql = f"""
                    CREATE TABLE IF NOT EXISTS {filename_dict['db_table_name']} (
                        label VARCHAR(30) PRIMARY KEY,
                        code VARCHAR(10) NOT NULL,
                        series VARCHAR(20),
                        generation VARCHAR(15),
                        component VARCHAR(40),
                        vendor VARCHAR(10) NOT NULL,
                        phase VARCHAR(20),
                        date DATETIME,
                        week int
                    );
                """,
            mysql_conn_id = filename_dict['mysql_conn_id'],
            trigger_rule = 'all_success'
        )
        
        save_to_mysql = PythonOperator(
            task_id = "save_to_mysql",
            python_callable = save_from_s3_to_mysql,
            op_kwargs = {            
                's3_bucket_name': filename_dict['s3_bucket_name'], 
                'filepath': filename_dict['filepath'], 
                'db_table_name': filename_dict['db_table_name'],  
                'insert_method': filename_dict['insert_method'], 
                'mysql_conn_id': filename_dict['mysql_conn_id']
            }
        )

        delete_temp_file = PythonOperator(
            task_id = "delete_temp_file",
            python_callable = delete_file_from_s3,
            op_kwargs = {  
                'bucket_name': filename_dict['s3_bucket_name'], 
                'bucket_key': filename_dict['filepath']
            }            
        )
        
        create_mysql_table >> save_to_mysql >> delete_temp_file

        return save_config_to_rds_task_group
import pandas as pd
from io import StringIO, BytesIO
import json
import boto3

from airflow.hooks.mysql_hook import MySqlHook
from airflow.providers.amazon.aws.hooks.lambda_function import AwsLambdaHook
from airflow.utils.db import provide_session
from airflow.models import XCom
from airflow.utils.state import State

import time


def check_file_type(filepath) :
    filename = filepath.split('/')[-1]

    if filename.lower().endswith('.csv'):
        return('csv')
    else:
        return('xlsx')


def check_is_xlsx(file_io: BytesIO) -> bool:

    xlsx_sig = b'\x50\x4B\x05\06'
    whence = 2
    offset = -22
    size = 4

    if type(file_io) == bytes or type(file_io) == BytesIO:
        if type(file_io) == bytes:
            obj_data = BytesIO(file_io)
        else:
            obj_data = file_io
        obj_data.seek(offset, whence)
        specific_bytes = obj_data.read(size)
        if specific_bytes == xlsx_sig:
            print('Valid XLSX file.')
            return True    
    
    print('Invalid XLSX file.')
    return False


def cleanup_files_in_folder(s3_bucket_name, filepath):
    s3 = boto3.resource('s3')
    s3_bucket = s3.Bucket(s3_bucket_name)
    s3_bucket.object_versions.filter(Prefix = filepath).delete()


def save_to_s3(dataframe: pd.DataFrame, bucket_name: str, file_path: str, opt_scientific_notion = None) -> None:
    s3 = boto3.client('s3')

    csv_buffer = StringIO()
    if opt_scientific_notion == None:
        dataframe.to_csv(csv_buffer, index = False)
    else:
        dataframe.to_csv(csv_buffer, index = False, float_format = '%.8f')
    s3.put_object(Bucket = bucket_name, Key = file_path, Body = csv_buffer.getvalue())


def move_file_from_s3_to_s3(origin_bucket: str, origin_key: str, dst_bucket: str, dst_key: str) -> None: 
    s3 = boto3.resource('s3')
    copy_source = {'Bucket' :origin_bucket,'Key' : origin_key }
    
    s3.meta.client.copy(copy_source, dst_bucket, dst_key)
    s3.meta.client.delete_object(Bucket = origin_bucket, Key = origin_key)


def delete_file_from_s3(bucket_name: str, bucket_key: str) -> None: 
    s3 = boto3.resource('s3')
    s3.meta.client.delete_object(Bucket = bucket_name, Key = bucket_key)

def clone_file_from_s3_to_s3(origin_bucket: str, origin_key: str, dst_bucket: str, dst_key: str) -> None: 
    s3 = boto3.resource('s3')
    copy_source = {'Bucket' :origin_bucket,'Key' : origin_key }
    
    s3.meta.client.copy(copy_source, dst_bucket, dst_key)

def check_file_exists_in_s3(s3_bucket_name: str, filepath: str, filter_path: list) -> bool:
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3_bucket_name)
    
    is_exist = []
    for path in filter_path:
        objs = list(bucket.objects.filter(Prefix = path))
        files_in_path= [item.key.split('/')[-1] for item in objs]
        is_exist.append((filepath in files_in_path))

    if any(is_exist):
        return True
    
    return False


def dataframe_to_sql(df: pd.DataFrame, db_table_name: str, insert_method:str = 'INSERT') ->str:  
    insert_method = insert_method.upper()
    assert insert_method in ['INSERT', 'REPLACE']
    
    data = []
    for ind, row in df.iterrows():
        tmp = map(str, row.to_list())
        tmp = map(lambda x: f"""'{x}'""", tmp)
        data.append(f"""({', '.join(tmp)})""")
    
    data = ', '.join(data)
    column_names = ', '.join(df.columns)
    insert_sql = f"""{insert_method} INTO {db_table_name} 
                        ({column_names}) 
                      VALUES 
                        {data};"""
    
    return insert_sql


def save_from_s3_to_mysql(s3_bucket_name:str, filepath:str, db_table_name:str, insert_method: str, 
                          mysql_conn_id: str) -> None:
    s3 = boto3.client('s3')
    s3_file = s3.get_object(Bucket = s3_bucket_name, Key = filepath)
    s3_file = pd.read_csv(s3_file['Body'])

    insert_sql = dataframe_to_sql(s3_file, db_table_name, insert_method)
    mysql = MySqlHook(mysql_conn_id = mysql_conn_id)
    mysql.run(insert_sql, autocommit=True)


def call_lambda_function(lambda_function_name, env, action, vendor, business, fileName,
                         success_task_id, fail_or_timeout_task_id, **kwargs):

    ssm = boto3.client('ssm')
    instanceIds = ssm.get_parameter(Name = f'/{env}/starliner/ssm-agent/{vendor}/instance-ids')['Parameter']['Value']
    payload = {'action': action, #'Succeed', 'Failed'
               'vendor': vendor, 
               'business': business, 
               'instanceIds': [instanceIds],
               'fileName': fileName}
    
    # check instance connection
    client = boto3.client('lambda')
    response = ssm.describe_instance_information(Filters=[{'Key': 'InstanceIds','Values': [instanceIds]}])
    print(response)
    connect_status = response['InstanceInformationList'][0]['PingStatus']
    
    if connect_status == 'Online':
        response = client.invoke(
            FunctionName = lambda_function_name, 
            InvocationType = "RequestResponse",
            Payload = json.dumps(payload))
        res = response['Payload'].read().decode()
        command_id = json.loads(res)['CommandId']
        print(command_id)
        time.sleep(2)
        
        command_response  = ssm.get_command_invocation(CommandId = command_id, InstanceId = instanceIds)
        while command_response['Status'] in ['Pending', 'InProgress']:
            # Others: ['Success', 'Cancelled', 'TimedOut', 'Failed', 'Cancelling']: 
            time.sleep(2)
            command_response  = ssm.get_command_invocation(CommandId = command_id, InstanceId = instanceIds)
        
        kwargs['ti'].xcom_push(key = 'command_id', value = command_id)
        kwargs['ti'].xcom_push(key = 'delivery_status', value = command_response['Status'])
        print(f"Command status: {command_response['Status']}")
        
        if command_response['Status'] == 'Success':
            print('Command Succeeded.')
            return success_task_id

        if command_response['Status'] == 'TimedOut':
            print('Instance timedout')
            return fail_or_timeout_task_id
        
        elif command_response['Status'] in ['Cancelled', 'Failed', 'Cancelling', 'Delayed']:
            print('Command failed. Publish command_id to queue for retry.')
            return fail_or_timeout_task_id
            # send mail to starliner
            # publish command id to queue for retry
            # save logs: vendor, instanceID....
        else: 
            return fail_or_timeout_task_id
            
    else:
        print('Instance Connection Failed.')
        return fail_or_timeout_task_id
        # send mail to vendor (default: starliner)
        # save logs: vendor, instanceID....
    

def save_lambda_result_to_dynamo(lambda_function_name, table_name, file_name, business, vendor, feature, 
                                 phase, delivery_status, processed_status, command_id, note, error_code, 
                                 dag_id, run_id, task_archive, task_failed, task_check,**kwargs):

    archive_state = (kwargs['dag_run'].get_task_instance(task_archive).state == State.FAILED)
    failed_state = (kwargs['dag_run'].get_task_instance(task_failed).state == State.FAILED)
    check_state = (kwargs['dag_run'].get_task_instance(task_check).state == State.FAILED)
    print(f'archive_state: {archive_state}, failed_state: {failed_state}, check_state: {check_state}')

    if not any([archive_state, failed_state, check_state]):
        client = boto3.client('lambda')
        payload = {
            'file_name': file_name,
            'table_name': table_name,
            'business': business,
            'vendor': vendor,
            'feature': feature,
            'phase': phase,
            'processed_status': processed_status, # fail_odm, fail_starliner, fail_vendor, success
            'delivery_status': delivery_status, # Pending, InProgress, Success, Cancelled, TimedOut, Failed, Cancelling
            'command_id': command_id, #kwargs['command_id'],
            'note': note, #kwargs['note'] # fail_odm, fail_starliner, fail_vendor, success
            'error_code': error_code,
            'dag_id': dag_id,
            'run_id': run_id
        }
        
        print(f'file_name: {file_name} \nbusiness: {business} \nvendor: {vendor} \nfeature: {feature}')
        print(f'table_name: {table_name}')
        print(f'phase: {phase} \nprocessed_status: {processed_status} \ndelivery_status: {delivery_status}')
        print(f'command_id: {command_id} \nnote: {note}')
        response = client.invoke(
            FunctionName = lambda_function_name, 
            InvocationType = "RequestResponse",
            Payload = json.dumps(payload)
        )


@provide_session
def cleanup_xcom(session = None, **context):
    dag = context['dag']
    dag_id = dag._dag_id
    execution_date = context['execution_date']

    session.query(XCom).filter(XCom.dag_id == dag_id, XCom.execution_date  == execution_date).delete()


def cleanup_processed_data(
                           bucket_name,
                           task_id_config, 
                           task_id_quality, 
                           filepath_processed_data,
                           filepath_processed_config,
                           filepath_processed_inspect,
                           **context):
    s3 = boto3.resource('s3')

    task_config = context['dag_run'].get_task_instance(task_id_config)
    task_quality = context['dag_run'].get_task_instance(task_id_quality)

    if task_config.state == State.FAILED:
        if len(list(s3.Bucket(bucket_name).objects.filter(Prefix = filepath_processed_data))) > 0:
            delete_file_from_s3(bucket_name, filepath_processed_data)
        if len(list(s3.Bucket(bucket_name).objects.filter(Prefix = filepath_processed_inspect))) > 0:
            delete_file_from_s3(bucket_name, filepath_processed_inspect)
    if task_quality.state == State.FAILED:
        if len(list(s3.Bucket(bucket_name).objects.filter(Prefix = filepath_processed_config))) > 0:
            delete_file_from_s3(bucket_name, filepath_processed_config)


def push_processed_status(note, processed_status, **context):
    context['task_instance'].xcom_push(key = 'note', value = note)
    context['task_instance'].xcom_push(key = 'processed_status', value = processed_status)


def list_s3_data(bucket_name,filepath):
    folderlist = []
    filelist = []

    client = boto3.client('s3')
    result = client.list_objects_v2(Bucket=bucket_name, Prefix=filepath, Delimiter='/')
    
    if 'CommonPrefixes' in result:
        for obj in result.get('CommonPrefixes'):
            prefix = obj.get('Prefix')
            folderlist.append(prefix)
    if 'Contents' in result:
        for obj in result.get('Contents'):
            filelist.append(obj)

    return folderlist,filelist
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor

from subdags.data_pipeline_common_utils import move_file_from_s3_to_s3, cleanup_processed_data, cleanup_xcom, push_processed_status, save_lambda_result_to_dynamo
from subdags.data_pipeline_handle_failure import handle_failure_task_group, write_exception_log
from subdags.data_pipeline_definition import CustomEail, Config
from subdags.validator import is_validate,get_spec_info
from subdags.transform_config import transform_main, save_config_to_mysql
from subdags.transform_data import main_transformation #, get_spec_info

from datetime import datetime


# Config 
DB_TABLE_NAME = Config.TABLE_CHASSIS_CONFIG
MYSQL_CONN_ID = Config.MYSQL_CONN_ID
LAMBDA_FUNCTION_NAME = Config.LAMBDA_FUNCTION_NAME
LOG_LAMBDA_FUNCTION_NAME = Config.LOG_LAMBDA_FUNCTION_NAME
DYNAMODB_TABLE_NAME = Config.DYNAMODB_TABLE_NAME
ENV = Config.ENV
ENV_LAMBDA = Config.ENV_LAMBDA 

TEST_VENDOR = Config.TEST_VENDOR
TEST_BUSINESS = Config.TEST_BUSINESS

TABLENAME_CODE = Config.TABLE_CODE
FOLDERPATH_SPEC = Config.FOLDERPATH_SPEC

# Failure Email
PLANET = CustomEail.PLANET
MAIL_TO = CustomEail.ERROR_MAIL


def get_file_info_from_event(**context):

    input_bucket = context["dag_run"].conf["bucketName"]
    input_key = context["dag_run"].conf["bucketKey"]
    input_filename_archive_fail = input_key.split('/')[-1]
    
    input_name = f"""{input_key.split('/')[-1].split('.')[0]}.csv""" 

    #push variables to xcoms
    context['task_instance'].xcom_push(key = 'input_bucket', value = input_bucket)
    context['task_instance'].xcom_push(key = 'input_key', value = input_key)
    context['task_instance'].xcom_push(key = 'input_filename_archive_fail', value = input_filename_archive_fail)
    context['task_instance'].xcom_push(key = 'input_name', value = input_name)

    FOLDERPATH_PROCESSED = '/'.join(input_key.split('/')[:-1]).replace(Config.LANDING_FOLDER, Config.PROCESSED_FOLDER) + "/"
    FOLDERPATH_PUBLISHED = '/'.join(input_key.split('/')[:-2]).replace(Config.LANDING_FOLDER, Config.PUBLISHED_FOLDER) + "/"
    FOLDERPATH_ARCHIVED = '/'.join(input_key.split('/')[:-1]).replace(Config.LANDING_FOLDER, Config.ARCHIVED_FOLDER) + "/"
    FOLDERPATH_FAILED = '/'.join(input_key.split('/')[:-1]).replace(Config.LANDING_FOLDER, Config.FAILED_FOLDER)+ "/"
    FOLDERPATH_CHECK = '/'.join(input_key.split('/')[:-1]).replace(Config.LANDING_FOLDER, Config.CHECK_FOLDER)+ "/"

    filepath_spec = get_spec_info(input_bucket,FOLDERPATH_SPEC, input_key)
    context['task_instance'].xcom_push(key = 'filepath_spec', value = filepath_spec)

    filepath_processed_data = f"{FOLDERPATH_PROCESSED}Data_{input_name}"
    context['task_instance'].xcom_push(key = 'filepath_processed_data', value = filepath_processed_data)

    filepath_processed_config = f"{FOLDERPATH_PROCESSED}Config_{input_name}"
    context['task_instance'].xcom_push(key = 'filepath_processed_config', value = filepath_processed_config)

    filepath_processed_inspect = f"{FOLDERPATH_PROCESSED}Inspect_{input_name}"
    context['task_instance'].xcom_push(key = 'filepath_processed_inspect', value = filepath_processed_inspect)

    filepath_archived_data = f"{FOLDERPATH_ARCHIVED}{input_filename_archive_fail}"
    context['task_instance'].xcom_push(key = 'filepath_archived_data', value = filepath_archived_data)

    filepath_failed_data = f"{FOLDERPATH_FAILED}{input_filename_archive_fail}"
    context['task_instance'].xcom_push(key = 'filepath_failed_data', value = filepath_failed_data)    

    filepath_check_data = f"{FOLDERPATH_CHECK}{input_filename_archive_fail}"
    context['task_instance'].xcom_push(key = 'filepath_check_data', value = filepath_check_data)

    if ENV == 'test':
        context['task_instance'].xcom_push(key = 'vendor', value = TEST_VENDOR)
        context['task_instance'].xcom_push(key = 'business', value = TEST_BUSINESS)
    else:
        context['task_instance'].xcom_push(key = 'vendor', value = input_key.split('/')[-2])
        context['task_instance'].xcom_push(key = 'business', value = input_key.split('/')[-3])        

    context['task_instance'].xcom_push(key = 'folderpath_processed', value = FOLDERPATH_PROCESSED)
    context['task_instance'].xcom_push(key = 'folderpath_published', value = FOLDERPATH_PUBLISHED)
    context['task_instance'].xcom_push(key = 'folderpath_archived', value = FOLDERPATH_ARCHIVED)
    context['task_instance'].xcom_push(key = 'folderpath_failed', value = FOLDERPATH_FAILED)



default_args = {
    'owner': 'felix',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2021, 1, 1),
    'on_failure_callback': write_exception_log
}


with DAG(dag_id = 'data_process_dag', 
         schedule_interval = None, 
         default_args = default_args, 
         catchup=False,
        ) as dag:

    check_file_sensor = S3KeySensor(
        task_id = 'check_new_file_s3',
        bucket_name = """{{dag_run.conf["bucketName"]}}""",
        bucket_key = """{{dag_run.conf["bucketKey"]}}""",
        timeout = 30 * 5,
        poke_interval = 30
    )
    
    get_file_info_from_event = PythonOperator(
        task_id = 'get_file_info_from_event',
        python_callable = get_file_info_from_event
    )

    
    S3_BUCKET_NAME = """{{task_instance.xcom_pull(key = 'input_bucket')}}"""
    FILEPATH_P_DIMENSION = """{{task_instance.xcom_pull(key = 'input_key')}}"""
    FILEPATH_SPEC = """{{task_instance.xcom_pull(key = 'filepath_spec')}}"""
    FILEPATH_PROCESSED_DATA = """{{task_instance.xcom_pull(key = 'filepath_processed_data')}}"""
    FILEPATH_PROCESSED_CONFIG = """{{task_instance.xcom_pull(key = 'filepath_processed_config')}}"""
    FILEPATH_PROCESSED_INSPECT = """{{task_instance.xcom_pull(key = 'filepath_processed_inspect')}}"""
    FILEPATH_ARCHIVED = """{{task_instance.xcom_pull(key = 'filepath_archived_data')}}"""
    FILEPATH_FAILED = """{{task_instance.xcom_pull(key = 'filepath_failed_data')}}"""
    FILEPATH_CHECK = """{{task_instance.xcom_pull(key = 'filepath_check_data')}}"""
    
    FOLDERPATH_PROCESSED = """{{task_instance.xcom_pull(key = 'folderpath_processed')}}"""
    FOLDERPATH_PUBLISHED = """{{task_instance.xcom_pull(key = 'folderpath_published')}}"""
    input_name = """{{task_instance.xcom_pull(key = 'input_name')}}"""

    VENDOR = """{{task_instance.xcom_pull(key = 'vendor')}}"""
    BUSINESS = """{{task_instance.xcom_pull(key = 'business')}}"""
    FILENAME = """{{task_instance.xcom_pull(key = 'input_filename_archive_fail')}}"""
    FEATURE = """{{task_instance.xcom_pull(key = 'input_filename_archive_fail').split('_')[4]}}""" #
    PHASE = """{{task_instance.xcom_pull(key = 'input_filename_archive_fail').split('_')[3]}}"""

    validate_data = PythonOperator(
        task_id = "validate_data",
        python_callable = is_validate,
        op_kwargs = {
            's3_bucket_name': S3_BUCKET_NAME,
            'filepath_quality_data': FILEPATH_P_DIMENSION,
            'mysql_conn_id': MYSQL_CONN_ID,
            'tablename_assembly_code': TABLENAME_CODE,
            'spec_bucket': S3_BUCKET_NAME, 
            'spec_key': FILEPATH_SPEC 
        }
    )


    with TaskGroup(group_id='transform_task_group') as transform_task_group:
        transform_config = PythonOperator(
            task_id = "transform_config",
            python_callable = transform_main,
            op_kwargs = {
                's3_bucket_name': S3_BUCKET_NAME,
                'filepath_quality_data': FILEPATH_P_DIMENSION,
                'filepath_processed': FILEPATH_PROCESSED_CONFIG,
                'filename' : input_name,
                'mysql_conn_id': MYSQL_CONN_ID,
                'tablename_code': TABLENAME_CODE
            }
        )

        transform_data = PythonOperator(
            task_id = "transform_data_p_dimension",
            python_callable = main_transformation,
            op_kwargs = {
                'quality_data_bucket': S3_BUCKET_NAME, 
                'quality_data_key': FILEPATH_P_DIMENSION, 
                'spec_bucket': S3_BUCKET_NAME, 
                'spec_key': FILEPATH_SPEC, 
                'filepath_processed_data': FILEPATH_PROCESSED_DATA,
                'filepath_processed_inspect': FILEPATH_PROCESSED_INSPECT
            }
        )
    

    handle_failure_task_group = handle_failure_task_group(
        dag = dag,
        trigger_rule = 'one_failed',
        params_dict = {
            'origin_bucket': S3_BUCKET_NAME, 
            'origin_key': FILEPATH_P_DIMENSION,
            'dst_bucket': S3_BUCKET_NAME,
            'dst_key_fail': FILEPATH_FAILED,
            'dst_key_check': FILEPATH_CHECK, 
            'lambda_function_name': LAMBDA_FUNCTION_NAME,
            'env': ENV_LAMBDA,
            'vendor': VENDOR,
            'business': BUSINESS,
            'fileName': FILENAME,
            'planet': PLANET,
            'lambda_mail_to': MAIL_TO,
            'task_id_config' : 'transform_task_group.transform_config',
            'task_id_quality' : 'transform_task_group.transform_data_p',
            'filepath_processed_data' : FILEPATH_PROCESSED_DATA,
            'filepath_processed_config' : FILEPATH_PROCESSED_CONFIG,
            'filepath_processed_inspect' : FILEPATH_PROCESSED_INSPECT
        }
    )



    filepath_published_quality_suffix = """{{task_instance.xcom_pull(key = 'filepath_published_quality_suffix')}}"""
    filepath_published_inspect_suffix = """{{task_instance.xcom_pull(key = 'filepath_published_inspect_suffix')}}"""
    
    

    FILEPATH_PUBLISHED_DATA = f"""{FOLDERPATH_PUBLISHED}{filepath_published_quality_suffix}Published_{input_name}"""
    FILEPATH_PUBLISHED_INSPECT = f"""{FOLDERPATH_PUBLISHED}{filepath_published_inspect_suffix}Inspect_{input_name}"""

    component = """{{task_instance.xcom_pull(key='component').lower().replace(' ', '_')}}"""
    DB_TABLE_NAME_NEW = f"{DB_TABLE_NAME}_{component}"

    with TaskGroup(group_id='save_data_task_group') as save_data_task_group:
        process_succeeded = PythonOperator(
            task_id = "process_succeeded", 
            python_callable = push_processed_status,
            trigger_rule = 'all_success',
            op_kwargs = {
                'processed_status': 'success',
                'note': 'success'
            }
        )

        save_data_to_s3 = PythonOperator(
            task_id = 'save_data_to_s3',
            python_callable = move_file_from_s3_to_s3,
            trigger_rule = 'all_success',
            op_kwargs = {
                'origin_bucket': S3_BUCKET_NAME, 
                'origin_key': FILEPATH_PROCESSED_DATA,
                'dst_bucket': S3_BUCKET_NAME, 
                'dst_key': FILEPATH_PUBLISHED_DATA 
            }
        )

        save_inspect_to_s3 = PythonOperator(
            task_id = 'save_inspect_to_s3',
            python_callable = move_file_from_s3_to_s3,
            trigger_rule = 'all_success',
            op_kwargs = {
                'origin_bucket': S3_BUCKET_NAME, 
                'origin_key': FILEPATH_PROCESSED_INSPECT,
                'dst_bucket': S3_BUCKET_NAME, 
                'dst_key': FILEPATH_PUBLISHED_INSPECT 
            }
        )

        save_config_to_rds = save_config_to_mysql(
            dag = dag,
            filename_dict = {
                's3_bucket_name': S3_BUCKET_NAME, 
                'filepath': FILEPATH_PROCESSED_CONFIG,
                'db_table_name': DB_TABLE_NAME_NEW, 
                'insert_method': 'REPLACE', 
                'mysql_conn_id': MYSQL_CONN_ID
            }
        )

        move_file_to_archive = PythonOperator(
            task_id = 'move_file_to_archive',
            python_callable = move_file_from_s3_to_s3,
            trigger_rule = 'all_success',
            op_kwargs = {
                'origin_bucket': S3_BUCKET_NAME, 
                'origin_key': FILEPATH_P_DIMENSION,
                'dst_bucket': S3_BUCKET_NAME, 
                'dst_key': FILEPATH_ARCHIVED
            }
        )

        process_succeeded >> [save_data_to_s3, save_inspect_to_s3, save_config_to_rds, move_file_to_archive]


    PROCESSED_STATUS = """{{task_instance.xcom_pull(key = 'processed_status')}}"""
    NOTE = """{{task_instance.xcom_pull(key = 'note')}}"""
    save_result_to_dynamo = PythonOperator(
        task_id = "save_record_to_dynamo",
        python_callable = save_lambda_result_to_dynamo,
        trigger_rule = 'all_done',
        op_kwargs = {
            'lambda_function_name': LOG_LAMBDA_FUNCTION_NAME,
            'table_name': DYNAMODB_TABLE_NAME,
            'file_name': FILENAME,
            'business': """{{task_instance.xcom_pull(key = 'input_key').split('/')[-3]}}""", #BUSINESS,
            'vendor': """{{task_instance.xcom_pull(key = 'input_key').split('/')[-2]}}""", #VENDOR,
            'feature': FEATURE,
            'phase': PHASE,
            'processed_status': PROCESSED_STATUS, # check, success, failed
            'delivery_status': """{{task_instance.xcom_pull(key = 'delivery_status')}}""", # Pending, InProgress, Success, Cancelled, TimedOut, Failed, Cancelling, None
            'command_id': """{{task_instance.xcom_pull(key = 'command_id')}}""", # command_id, None
            'note': NOTE, 
            'error_code': """{{task_instance.xcom_pull(key = 'exception_log')}}""",
            'dag_id': """{{dag.dag_id}}""",
            'run_id': """{{dag_run.run_id}}""",
            'task_archive': 'save_data_task_group.move_file_to_archive', 
            'task_failed': 'handle_failure_task_group.move_file_to_failed', 
            'task_check': 'handle_failure_task_group.clone_file_to_check'
        }
    )
   
    cleanup_xcom = PythonOperator(
        task_id = "clean_xcom",
        python_callable = cleanup_xcom,
        provide_context = True, 
        trigger_rule = 'all_done'
    )
    
    for_dag_state = DummyOperator(
        task_id = "for_dag_state",
        trigger_rule = 'none_failed_or_skipped')

    check_file_sensor >> get_file_info_from_event >> validate_data >> transform_task_group
    validate_data >> handle_failure_task_group
    transform_task_group >> [save_data_task_group, handle_failure_task_group]  
    save_data_task_group >> handle_failure_task_group >> save_result_to_dynamo >> cleanup_xcom

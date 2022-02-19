import pandas as pd

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.task_group import TaskGroup

from subdags.data_pipeline_common_utils import move_file_from_s3_to_s3, call_lambda_function, cleanup_processed_data, push_processed_status, clone_file_from_s3_to_s3


def write_exception_log(context):
    exception =  str(context.get('exception'))
    context['task_instance'].xcom_push(key = 'exception_log', value = exception)


def classify_failure_causes(**context):
    exception = context['task_instance'].xcom_pull(key = 'exception_log')

    if exception.startswith('VendorDataError'):
        return 'handle_failure_task_group.fail_vendor'

    elif exception.startswith('OdmDataError'):
        return 'handle_failure_task_group.fail_odm'

    else:
        return 'handle_failure_task_group.fail_starliner'


def handle_failure_task_group(dag: DAG, params_dict: dict, trigger_rule: str) -> TaskGroup:
    with TaskGroup(group_id = 'handle_failure_task_group') as handle_failure_task_group: 
        cleanup_data_in_processed = PythonOperator(
            task_id = 'cleanup_files_in_processed',
            dag = dag,
            python_callable = cleanup_processed_data,
            trigger_rule = trigger_rule,
            op_kwargs = {
                'bucket_name': params_dict['origin_bucket'], 
                'task_id_config': params_dict['task_id_config'], 
                'task_id_quality': params_dict['task_id_quality'], 
                'filepath_processed_data': params_dict['filepath_processed_data'],
                'filepath_processed_config': params_dict['filepath_processed_config'], 
                'filepath_processed_inspect': params_dict['filepath_processed_inspect'] 
            } 
        )

        handle_failure_action = BranchPythonOperator(
            task_id = 'handle_failure_action',
            dag = dag, 
            python_callable = classify_failure_causes,
            trigger_rule = trigger_rule,
        )

        fail_starliner = PythonOperator(
            task_id = "fail_starliner", 
            dag = dag,
            python_callable = push_processed_status,
            op_kwargs = {
                'processed_status': 'check',
                'note': 'fail_starliner'
            }
        )

        fail_odm = PythonOperator(
            task_id = "fail_odm", 
            dag = dag,
            python_callable = push_processed_status,
            op_kwargs = {
                'processed_status': 'failed',
                'note': 'fail_odm'
            }
        )

        fail_vendor = PythonOperator(
            task_id = "fail_vendor", 
            dag = dag,
            python_callable = push_processed_status,
            op_kwargs = {
                'processed_status': 'failed',
                'note': 'fail_vendor'
            }
        )

        move_file_to_failed = PythonOperator(
            task_id = "move_file_to_failed", 
            python_callable = move_file_from_s3_to_s3,
            dag = dag, 
            trigger_rule = 'none_failed_or_skipped',
            op_kwargs = {
                'origin_bucket': params_dict['origin_bucket'], 
                'origin_key': params_dict['origin_key'], 
                'dst_bucket': params_dict['dst_bucket'], 
                'dst_key': params_dict['dst_key_fail']
            } 
        )

        clone_file_to_check = PythonOperator(
            task_id = "clone_file_to_check", 
            python_callable = clone_file_from_s3_to_s3,
            dag = dag, 
            op_kwargs = {
                'origin_bucket': params_dict['origin_bucket'], 
                'origin_key': params_dict['origin_key'], 
                'dst_bucket': params_dict['dst_bucket'], 
                'dst_key': params_dict['dst_key_check'] 
            } 
        )

        call_lambda_function_fail = call_lambda_task_group(
            dag = dag,
            group_id = 'call_lambda_function_fail_task_group',
            parent_group_id = 'handle_failure_task_group',
            trigger_rule = 'none_failed_or_skipped', 
            params_dict = {
                'lambda_function_name': params_dict['lambda_function_name'],
                'env': params_dict['env'],
                'action': 'Failed',
                'vendor': params_dict['vendor'],
                'business': params_dict['business'],
                'fileName': params_dict['fileName'],
                'planet': params_dict['planet'],
                'lambda_mail_to': params_dict['lambda_mail_to']
            }
        )

        handle_failure_action >> fail_starliner >> clone_file_to_check
        handle_failure_action >> fail_odm >> move_file_to_failed >> call_lambda_function_fail
        handle_failure_action >> fail_vendor >> move_file_to_failed >> call_lambda_function_fail


        return handle_failure_task_group


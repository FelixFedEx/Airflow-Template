import pandas as pd
import re

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from subdags.data_pipeline_definition import CustomError
from subdags.data_pipeline_common_utils import list_s3_data


def is_validate(s3_bucket_name, filepath_data, mysql_conn_id, **kwargs) -> None:
    from subdags.transform_config import read_data
    
    # Examine filename
    filename = filepath_data.split('/')[-1]
    filename_split = filename.split('_')

    assert filename.lower().endswith('.csv') or filename.lower().endswith('.xlsx'), f'Error: File type not csv or xlsx.'
    assert len(filename_split) == 7, f'Error: File name length not 7.'
    assert len(filename_split[2])==10, f'Error: Label length not 10.'
    
    # Read file and its assembly code
    obj_quality_data, _ = read_data(s3_bucket_name, filepath_data, mysql_conn_id, 'tablename_code')

    # Validate data
    assert obj_quality_data.odm_label.str.startswith(filename_split[2]).all(), f'Error: Data not having same length.'
    assert obj_quality_data.notnull().all().all(), f'Error: Missing values.'
    

def get_spec_info(bucket_name,FOLDERPATH_SPEC, input_key):
    filename = input_key.split('/')[-1]
    code = filename.split('_')[2][:5]
    phase = filename.split('_')[3]
    folders, spec_list = list_s3_data(bucket_name, FOLDERPATH_SPEC)
    spec_key = find_last_spec(code, phase,spec_list)
    return spec_key

def find_last_spec(code,phase,candidate_list):
    last_spec_key=None

    filepattern = f'{phase}(.*).xlsx'
    pattern = re.compile(filepattern)

    match_spec =[]
    for value in candidate_list:
        filename = value['Key'].split('/')[-1]
        if pattern.match(filename):
            match_spec.append(value)
    if len(match_spec)>0:
        time_lst =[]
        for i, val in enumerate(match_spec):
            time_lst.append(val['LastModified'])
        last_file_index = time_lst.index(max(time_lst))
        last_spec_key = match_spec[last_file_index]['Key']

    return last_spec_key
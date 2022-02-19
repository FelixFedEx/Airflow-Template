import pandas as pd
import numpy as np
from io import StringIO
import io
  
from airflow import DAG  
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup

from subdags.transform_config import get_regularized_column_header,get_regularized_data
from subdags.data_pipeline_common_utils import save_to_s3, check_file_type
from subdags.data_pipeline_definition import CustomError

from datetime import datetime, timedelta
import boto3
import botocore
import uuid



def check_file_exist(bucket_name, bucket_key):

    if bucket_key=="None" or bucket_key is None:
        return False

    s3 = boto3.resource('s3')

    try:
        s3.Object(bucket_name, bucket_key).load()
        return True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return False


def get_ballmark_matching(df_, df_spec):
    spec_mark = df_spec['Balloon'].to_list()
    _header = df_.columns.to_list()
    if len(set([i for i in _header if i in spec_mark ])) == len(spec_mark):
        return True
    else:
        return False



def get_spec_info(spec_folder_path, data_match_spec):
    odm_5digits_phase = data_match_spec.split('_')[-5][:5] +'_'+ data_match_spec.split('_')[-4]
    spec_name = odm_5digits_phase + '_Spec.xlsx'
    spec_key_ = spec_folder_path + spec_name

    return spec_key_


def connection_s3_bucket_object(f_data_bucket, f_data_key, f_spec_bucket, f_spec_key):
    s3 = boto3.client('s3')
    obj_main_ = s3.get_object(Bucket = f_data_bucket, Key = f_data_key )
    obj_spec_ = s3.get_object(Bucket = f_spec_bucket, Key = f_spec_key)
    
    filetype_ = check_file_type(f_data_key)

    return obj_main_, obj_spec_, filetype_




def read_spec(fileobj_data, fileobj_spec, _type_):
    list_column = ['P Size', '+Tol', '-Tol']
    list_column_header = ['Balloon', '* means for distribution', 'P Size', '+Tol', '-Tol']

    df_spec = pd.read_excel(io.BytesIO(fileobj_spec.read()), sheet_name = 'dimension', usecols = 'A:E').dropna(how = 'all')

    assert (df_spec[list_column] >= 0).all().all(), f'There are some negative values in the spec.'   
    assert len([x for x in df_spec.columns.to_list() if x in list_column_header]) == len(list_column_header), f'Missing the columns in spec '    
    
    df_spec[df_spec.columns[1]].fillna('NA', inplace = True)

    df_spec['upper_limit'] = df_spec['P Size'] + df_spec['+Tol']
    df_spec['lower_limit'] = df_spec['P Size'] - df_spec['-Tol']
    spec_mark = df_spec['Balloon'].to_list()

    #
    if _type_ == 'csv':
        df_ = pd.read_csv(fileobj_data, skiprows = [0, 1, 3, 4, 5, 6]).replace('^\s*$', np.nan, regex = True).dropna(how = 'all')
    elif _type_ == 'xlsx':
        df_ = pd.read_excel(io.BytesIO(fileobj_data.read()), skiprows = [0, 1, 3, 4, 5, 6]).replace('^\s*$', np.nan, regex = True).dropna(how = 'all')
    
    # rename column header
    column_mapping = {df_.columns[0]: 'Time', df_.columns[1]: 'Label', df_.columns[2]: 'Result'}
    df_.rename(columns = column_mapping, inplace = True)

    df_ = get_regularized_data(df_,'Label')
    df_.rename(columns = lambda x: x.strip() if type(x)== str else x , inplace = True)
    
    if df_['Time'].dtypes == 'object':
        df_['Time'] = df_['Time'].astype('datetime64[ns]')

    row_missing_idx = df_[df_['Label'].isnull() | df_['Time'].isnull() ].index.tolist()
    
    if len(row_missing_idx) > 0:
        df_.drop(row_missing_idx, axis = 0, inplace = True)

    df_['record_id'] = df_.apply(lambda x:uuid.uuid4(), axis = 1)
    column_list = ['Time', 'Label', 'record_id'] + spec_mark
    
    #check the existing data corresponding to the ballmark in spec
    assert get_ballmark_matching(df_, df_spec), f'Error: data misses some ballmarks responding to spec.'

    df_ = df_.loc[:, column_list]
    df_spec = df_.loc[:, ['record_id', 'Label', 'Time']]
  
    assert df_.notnull().values.all(), f'Error: There are some missing values of measurement in  data'

    return df_, df_spec, df_spec 


def transform_inspect_activity(df, filepath):
    
    df['file_name'] = filepath.split('/')[-1]
    df['machine'] = [i[0] for i in df['file_name'].apply(lambda x:x.split('_')[-1]).str.split('.')]
    df['spec'] = f"{filepath.split('/')[-1].split('_')[2][:5]}_{filepath.split('/')[-1].split('_')[3]}_Spec.xlsx"
    # df['file_stamp'] = [i[-2:-1][0] for i in df['file_name'].str.split('_')]

    return df



def transform__data(file__data, df_spec_, df):

    df_ = file__data.set_index(['Label','Time', 'record_id']).stack().reset_index()
    df_.rename(columns = {df_.columns[3]: 'Balloon', df_.columns[4]:'value'}, inplace = True)
    
    
    #merge dataframe with spec
    df_ = pd.merge(df_, df_spec_, on = "Balloon", how = 'left')
    
    #get serial number without last 5 digits
    df_['basic_info'] = df_['Label'].str[0:13]
    
    # get status
    df_['status_by_value'] = df_.apply(lambda x: 'pass' if x['value'] <= x['upper_limit'] and x['value'] >= x['lower_limit'] else 'fail', axis = 1 )
    
    df_ = df_.rename(columns= {'* means for distribution': 'check_distribution',
                                      '+Tol' : 'plus_tol',
                                      '-Tol' : 'minus_tol',
                                      'upper_limit':'spec_usl', 
                                      'lower_limit':'spec_lsl', 
                                      'P Size':'spec_target'})
    
    df_status = df_.groupby(['Label', 'Time', 'record_id']).apply(lambda x: False if all(x['status_by_value'] == 'pass') == False else True).reset_index().rename(columns= {0: 'status'})
    df = df.merge(df_status, on = ['Label', 'Time', 'record_id'], how = 'left') 

    df_ = get_regularized_column_header(df_)
    df_spec = get_regularized_column_header(df)
    
    return df_, df_spec


def main_transformation(data_bucket, _data_key, spec_bucket, spec_key, filepath_processed_data, filepath_processed_inspect):
    
    obj_main, obj_spec, filetype = connection_s3_bucket_object(data_bucket, _data_key, spec_bucket, spec_key)
    
    df_q, df_i_a, df_s = read_spec(obj_main['Body'], obj_spec['Body'], filetype)
  
    df_i_a = transform_inspect_activity(df_i_a, _data_key)

    # data transformation
    df_, df_spec = transform__data(df_q, df_s, df_i_a)


    #check missing value  
    assert df_.notnull().values.all(), f'Error: data has missing values.'
    assert df_spec.notnull().values.all(), f'Error: InspectActivity has missing values.'
    
   

    # push cleaned data to the processed folder
    save_to_s3(df_, data_bucket, filepath_processed_data, "no_scientific_notion")
    save_to_s3(df_spec, data_bucket, filepath_processed_inspect)

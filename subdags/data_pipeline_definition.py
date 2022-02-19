from airflow.models import Variable

class CustomError:
    ERROR_NO_SPEC = "0x10001"
    ERROR_NO_ASSEMBLY = "0x10002"
    ERROR_FILE_EXTENSION = "0x10003"
    ERROR_FILENAME_START = "0x10004"
    ERROR_FILENAME_FEATURE = "0x10005"
    ERROR_FILENAME_SECTION = "0x10006"
    ERROR_FILENAME_END = "0x10007"
    ERROR_READ_FILE = "0x10008"
    ERROR_BASIC_INFORMATION = "0x10009"
    ERROR_SPEC_MISSING_VALUE = "0x1010"
    ERROR_FILE_MISSING_VALUE = "0x1011"
    ERROR_FILE_SHOULD_NOT_EXIST = "0x1012"
    ERROR_FILE_WRONG_VALUE_TYPE = "0x1013"
    ERROR_FILE_MISSING_BALLMARK = "0x1014"
    ERROR_SPEC_WRONG_ITEM_POINT = "0x1015"

    ERROR_INCONSISTENCY_SERIES = "0x20001"
    ERROR_INCONSISTENCY_GENERATION = "0x20002"
    ERROR_INCONSISTENCY_SIZE = "0x20003"
    ERROR_INCONSISTENCY_LAYOUT = "0x20004"
    ERROR_INCONSISTENCY_PHASE = "0x20005"
    ERROR_INCONSISTENCY_VENDOR = "0x20006"


class CustomEail:
    PLANET = 'VENUS'
    airflow_variable = Variable.get("me/email_list", deserialize_json = True)

    ERROR_MAIL = airflow_variable.pop('us_contact_list')
    STAKEHOLDER_EMAIL = airflow_variable


class Config:
    airflow_variable = Variable.get("me/env_variables", deserialize_json = True) # read from AWS secrete manager

    MYSQL_CONN_ID = airflow_variable['me_rds_database'] # output: 'me_database'
    TABLE_CODE = 'common_code'

    # chassis
    TABLE_CHASSIS_CONFIG =  'chassis_config'

    # environment variables
    ENV = airflow_variable["environment"] 
    LAMBDA_FUNCTION_NAME =  airflow_variable["lambda-event-trigger-run-command"] 
    ENV_LAMBDA = 'dev' 
    LOG_LAMBDA_FUNCTION_NAME = airflow_variable["lambda-dynamodb-writer"] 
    DYNAMODB_TABLE_NAME = airflow_variable["dynamodb-me-table"] 

    # test params
    TEST_VENDOR = 'test_vendor'
    TEST_BUSINESS = 'test_business'

    
    # folders
    FOLDERPATH_SPEC = 'Common-Table/Spec/'

    ME_PROD_S3_BUCKET = "stella-datalake-dev"
    FOLDERPATH_ME_KB_WEEKLY = "ME/keyboard/Auto-Feeling/WeeklyQuality/"

    LANDING_FOLDER = "landing"
    PROCESSED_FOLDER = 'processed'
    PUBLISHED_FOLDER = 'published'
    ARCHIVED_FOLDER = 'archive'
    FAILED_FOLDER = 'failed'
    CHECK_FOLDER = 'check'
    
    SPEC_ARCHIVE_FOLDER = 'archive'
    SPEC_FAIL_FOLDER = 'fail'
    SPEC_ERROR_FOLDER = 'error'

    # reports
    REPORT_S3_BUCKET_NAME = airflow_variable["s3_bucket_name"] 
    DAILY_REPORT_PATH = airflow_variable["daily_report_path"] 
    REPORT_START_HOUR = 19
    REPORT_START_MINUTE = 30


    MAP_YEAR = {'5': 2005, '6': 2006, '7': 2007, '8': 2008, '9': 2009, 'A': 2010, 'B': 2011,
                'C': 2012, 'D': 2013, 'E': 2014, 'F': 2015, 'G': 2016, 'H': 2017, 'J': 2018,
                'K': 2019, 'L': 2020, 'M': 2021, 'N': 2022, 'P': 2023, 'Q': 2024, 'R': 2025,
                'S': 2026, 'T': 2027, 'U': 2028, 'V': 2029, 'W': 2030, 'X': 2031, 'Y': 2032,
                'Z': 2033, '2': 2034, '3': 2035}

    MAP_MONTH = {'1': 1, '2': 2, '3': 3, '4': 4, '5': 5, '6': 6, '7': 7, '8': 8, '9': 9, 'A': 10,
                 'B': 11, 'C': 12}

    MAP_DAY = {'1': 1, '2': 2, '3': 3, '4': 4, '5': 5, '6': 6, '7': 7, '8': 8, '9': 9, 'A': 10,
               'B': 11, 'C': 12, 'D': 13, 'E': 14, 'F': 15, 'G': 16, 'H': 17, 'J': 18, 'K': 19,
               'L': 20, 'M': 21, 'N': 22, 'P': 23, 'R': 24, 'S': 25, 'T': 26, 'V': 27, 'W': 28,
               'X': 29, 'Y': 30, 'Z': 31}
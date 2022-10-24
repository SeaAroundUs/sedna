import boto3
import os
from dotenv import load_dotenv

load_dotenv()  # bring in values from .env

# shared
SEDNA_TAGS = [{'TagKey': 'project', 'TagValue': 'sedna'}]
SEDNA_ALT_TAGS = [{'Key': 'project', 'Value': 'sedna'}]
REGION_NAME = os.getenv('REGION_NAME', 'us-west-2')
ACCOUNT_ID = boto3.client('sts').get_caller_identity()['Account']

# s3
BUCKET_NAME = os.getenv('BUCKET_NAME', 'sedna-catshark-storage')

# rds
EXPORT_S3_PATH = 'sedna-exports'
EXPORT_DATABASE = os.getenv('EXPORT_DATABASE', 'seaaroundus')
try:
    with open('.export_version') as f:
        EXPORT_TASK_NAME = f'sedna-sau-int-export-{f.readline().strip()}'
except OSError:
    raise Exception(f'Missing .export_version file; please use the configuration described in README.md')
PARQUET_PREFIX = f'{EXPORT_S3_PATH}/{EXPORT_TASK_NAME}/{EXPORT_DATABASE}'
DATABASE_ID = 'sedna-catshark-dev'
SNAPSHOT_ID = 'sedna-snapshot'
EXPORT_DB_PATH = [
    f'{EXPORT_DATABASE}.allocation',
    f'{EXPORT_DATABASE}.distribution',
    f'{EXPORT_DATABASE}.geo',
    f'{EXPORT_DATABASE}.master',
    f'{EXPORT_DATABASE}.recon',
]

# direct db connection
EXPORT_HOST = os.getenv('EXPORT_HOST')
EXPORT_USER = os.getenv('EXPORT_USER')
EXPORT_PASSWORD = os.getenv('EXPORT_PASSWORD')

# iam
EXPORT_POLICY_NAME = 'sedna-export-policy'
EXPORT_ROLE_NAME = 'sedna-export-role'
RDS_TO_S3_POLICY_NAME = 'sedna-rds-to-s3-policy'
RDS_TO_S3_ROLE_NAME = 'sedna-rds-to-s3-role'

# kms
EXPORT_KEY_NAME = 'sedna-export-key'
KEY_DESCRIPTION = 'Key for encrypting RDS snapshot exports to S3'

# athena
RESULT_CONFIGURATION = {'OutputLocation': f's3://{BUCKET_NAME}/query_results/'}
ALLOCATION_RESULT_PREFIX = f'{EXPORT_S3_PATH}/{EXPORT_TASK_NAME}/allocation_result'
ALLOCATION_SUPPORT_TABLES = ['simple_area_cell_assignment', 'allocation_simple_area', 'cell', 'data']
# TODO make the athena database name a passable value so we support more than one run at once?


def read_sql_file(filename, replace=True, **kwargs):
    this_path = os.path.dirname(__file__)
    abs_path = os.path.join(this_path, '../sql', filename)
    fd = open(abs_path)
    sql = fd.read()
    if replace:
        kwargs.update({'BUCKET_NAME': BUCKET_NAME, 'PARQUET_PREFIX': PARQUET_PREFIX})
        sql = sql.format(**kwargs)
    return sql


def check_env_file():
    env_vals = [REGION_NAME, BUCKET_NAME, EXPORT_DATABASE, EXPORT_HOST, EXPORT_USER, EXPORT_PASSWORD]
    if any(not v for v in env_vals):
        raise Exception(f'One or more .env variables are unset; please use the configuration described in README.md')

import os

# shared
SEDNA_TAGS = [{'TagKey': 'project', 'TagValue': 'sedna'}]
SEDNA_ALT_TAGS = [{'Key': 'project', 'Value': 'sedna'}]
REGION_NAME = 'us-west-2'

# s3
BUCKET_NAME = 'sedna-catshark-storage'

# athena
RESULT_CONFIGURATION = {'OutputLocation': f's3://{BUCKET_NAME}/query_results/'}

# rds
EXPORT_S3_PATH = 'sedna-exports'
with open('export_version') as f:
    EXPORT_TASK_NAME = f'sedna-sau-int-export-{f.readline().strip()}'
PARQUET_PREFIX = f'{EXPORT_S3_PATH}/{EXPORT_TASK_NAME}/seaaroundus'  # TODO change to sau_int on prod
DATABASE_ID = 'sedna-catshark-dev'
SNAPSHOT_ID = 'sedna-snapshot'
EXPORT_DB_PATH = [  # TODO change to sau_int on prod
    'seaaroundus.allocation',
    'seaaroundus.distribution',
    'seaaroundus.geo',
    'seaaroundus.master',
    'seaaroundus.recon',
]

# iam
EXPORT_POLICY_NAME = 'sedna-export-policy'
EXPORT_ROLE_NAME = 'sedna-export-role'

# kms
EXPORT_KEY_NAME = 'sedna-export-key'
KEY_DESCRIPTION = 'Key for encrypting RDS snapshot exports to S3'


def read_sql_file(filename, replace=True):
    this_path = os.path.dirname(__file__)
    abs_path = os.path.join(this_path, '../sql', filename)
    fd = open(abs_path)
    sql = fd.read()
    if replace:
        sql = sql.format(BUCKET_NAME=BUCKET_NAME, PARQUET_PREFIX=PARQUET_PREFIX)
    return sql

import os

# shared
SEDNA_TAGS = [{'TagKey': 'project', 'TagValue': 'sedna'}]
REGION_NAME = 'us-west-2'

# s3
BUCKET_NAME = 'sedna-catshark-storage'
EXPORT_S3_PATH = 'sedna-exports'

# rds
RESULT_CONFIGURATION = {'OutputLocation': 's3://{}/query_results/'.format(BUCKET_NAME)}
PARQUET_PREFIX = '{}/sedna-sau-int-export/seaaroundus/'.format(EXPORT_S3_PATH)
SNAPSHOT_NAME = 'sedna-catshark-dev'
EXPORT_DB_PATH = [  # TODO change to sau_int on prod
    'seaaroundus.allocation',
    'seaaroundus.distribution',
    'seaaroundus.geo',
    'seaaroundus.master',
    'seaaroundus.recon',
]
EXPORT_TASK_NAME = 'sedna-sau-int-export'

# iam
EXPORT_POLICY_NAME = 'sedna-export-policy'
EXPORT_ROLE_NAME = 'sedna-export-role'

# kms
EXPORT_KEY_NAME = 'sedna-export-key'
KEY_DESCRIPTION = 'Key for encrypting RDS snapshot exports to S3'


def read_sql_file(filename):
    this_path = os.path.dirname(__file__)
    abs_path = os.path.join(this_path, '../sql', filename)
    fd = open(abs_path)
    return fd.read()

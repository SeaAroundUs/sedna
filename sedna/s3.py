import botocore.exceptions as boto_exceptions
import boto3
from sedna.common import BUCKET_NAME


# check for existence of sedna bucket
def check_for_sedna_bucket():
    s3 = boto3.client('s3')
    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
    except boto_exceptions.ClientError:
        raise Exception(f'Cannot access bucket "{BUCKET_NAME}"; please use the configuration described in README.md')


# check if directory exists and is not empty
def folder_exists_and_not_empty(path):
    s3 = boto3.client('s3')
    resp = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=path, MaxKeys=1)
    return 'Contents' in resp


def get_csv_file_in_folder(path):
    s3 = boto3.client('s3')
    resp = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=path)
    for file in resp['Contents']:
        file_name = file['Key']
        if file_name[-3:] == 'csv':
            return file_name
    raise Exception(f'No CSV found in {path}')

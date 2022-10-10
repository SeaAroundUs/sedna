import boto3
from sedna.common import BUCKET_NAME


# check for existence of sedna bucket
def check_for_sedna_bucket():
    pass  # TODO


# check if directory exists and is not empty
def folder_exists_and_not_empty(path):
    s3 = boto3.client('s3')
    resp = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=path, MaxKeys=1)
    return 'Contents' in resp

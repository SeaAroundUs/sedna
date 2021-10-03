import boto3
from s3 import BUCKET_NAME
from iam import EXPORT_ROLE_ARN

SNAPSHOT_NAME = 'sedna-catshark-dev'
EXPORT_KMS_ID = ''  # TODO
EXPORT_S3_PATH = ''  # TODO
EXPORT_DB_PATH = 'seaaroundus'  # TODO might be sau_int on prod


# TODO break this up into multiple functions
def do_snapshot_stuff():
    rds = boto3.client('rds')

    # check for ongoing tasks
    res = rds.describe_export_tasks()
    print(res['ExportTasks'])

    # grab snapshot ARN
    res = rds.describe_db_snapshots(DBInstanceIdentifier=SNAPSHOT_NAME)
    snapshot_arn = res['DBSnapshots'][0]['DBSnapshotArn']

    # TODO create KMS key if doesn't exist:
    #  https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.Encryption.html

    # start export task
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Client.start_export_task
    response = rds.start_export_task(
        ExportTaskIdentifier='sedna_default_export',
        SourceArn=snapshot_arn,
        S3BucketName=BUCKET_NAME,
        IamRoleArn=EXPORT_ROLE_ARN,
        KmsKeyId='TODO',
        S3Prefix=EXPORT_S3_PATH,
        ExportOnly=[EXPORT_DB_PATH]
    )

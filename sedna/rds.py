import boto3
from sedna.common import BUCKET_NAME, REGION_NAME, EXPORT_S3_PATH, \
    SNAPSHOT_NAME, EXPORT_DB_PATH, EXPORT_TASK_NAME


def should_start_export(snapshot_arn):
    rds = boto3.client('rds', region_name=REGION_NAME)
    res = rds.describe_export_tasks(SourceArn=snapshot_arn)
    # TODO check for export or not
    # from pprint import pp
    # pp(res)
    return False  # TODO check for reals


def get_or_create_snapshot():
    rds = boto3.client('rds', region_name=REGION_NAME)
    # TODO create snapshot
    res = rds.describe_db_snapshots(DBInstanceIdentifier=SNAPSHOT_NAME)
    return res['DBSnapshots'][0]['DBSnapshotArn']


def start_export(snapshot_arn, role_arn, kms_key_id):
    rds = boto3.client('rds', region_name=REGION_NAME)
    return rds.start_export_task(
        ExportTaskIdentifier=EXPORT_TASK_NAME,
        SourceArn=snapshot_arn,
        S3BucketName=BUCKET_NAME,
        IamRoleArn=role_arn,
        KmsKeyId=kms_key_id,
        S3Prefix=EXPORT_S3_PATH,
        ExportOnly=EXPORT_DB_PATH
    )

# TODO will also need to export a CSV of allocation.v_internal_generate_allocation_simple_area_table

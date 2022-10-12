import botocore.exceptions as boto_exceptions
import boto3
import psycopg2
import re
from sedna.common import BUCKET_NAME, REGION_NAME, EXPORT_S3_PATH, DATABASE_ID, \
    SNAPSHOT_ID, EXPORT_DB_PATH, EXPORT_TASK_NAME,  EXPORT_HOST, EXPORT_USER, \
    EXPORT_PASSWORD, EXPORT_DATABASE, PARQUET_PREFIX, SEDNA_ALT_TAGS

VIEWS_FOR_EXPORT = [
    'allocation.v_internal_generate_allocation_simple_area_table',
]


def export_views():
    conn = psycopg2.connect(
        host=EXPORT_HOST,
        user=EXPORT_USER,
        password=EXPORT_PASSWORD,
        dbname=EXPORT_DATABASE
    )
    with conn.cursor() as cur:
        cur.execute('CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE;')
        for view in VIEWS_FOR_EXPORT:
            short_view_name = re.sub(r'(^\w+\.(v_)?)', '', view)
            cur.execute(f'''
                SELECT * FROM aws_s3.query_export_to_s3(
                    'SELECT * FROM {view}',
                    aws_commons.create_s3_uri(
                        '{BUCKET_NAME}',
                        '{PARQUET_PREFIX}/views.{short_view_name}/view.csv',
                        '{REGION_NAME}'
                    ),
                    options :='FORMAT CSV'
                );        
            ''')


def get_or_create_snapshot():
    rds = boto3.client('rds', region_name=REGION_NAME)
    res = rds.describe_db_snapshots(DBSnapshotIdentifier=SNAPSHOT_ID, DBInstanceIdentifier=DATABASE_ID)
    if res['DBSnapshots']:
        if res['DBSnapshots'][0]['Status'] == 'creating':  # TODO handle errors on snapshot, case sensitivity
            print('Snapshot still being created...\nPlease keep waiting!')
            exit(0)
        return res['DBSnapshots'][0]['DBSnapshotArn']
    else:
        rds.create_db_snapshot(
            DBSnapshotIdentifier=SNAPSHOT_ID,
            DBInstanceIdentifier=DATABASE_ID,
            Tags=SEDNA_ALT_TAGS
        )
        print(f'Now creating snapshot {SNAPSHOT_ID}...\nPlease re-run the script in ~15 minutes!')
        exit(0)


def get_or_create_export(snapshot_arn, role_arn, kms_key_id):
    rds = boto3.client('rds', region_name=REGION_NAME)
    res = rds.describe_export_tasks(ExportTaskIdentifier=EXPORT_TASK_NAME, SourceArn=snapshot_arn)
    if res['ExportTasks']:
        if res['ExportTasks'][0]['Status'] in ['STARTING', 'IN_PROGRESS']:  # TODO handle errors, case sensitivity
            print('Export still running...\nPlease keep waiting!')
            exit(0)
        return None
    else:
        print(f'Starting export task {EXPORT_TASK_NAME}\nPlease re-run the script in ~15 minutes!')
        rds.start_export_task(
            ExportTaskIdentifier=EXPORT_TASK_NAME,
            SourceArn=snapshot_arn,
            S3BucketName=BUCKET_NAME,
            IamRoleArn=role_arn,
            KmsKeyId=kms_key_id,
            S3Prefix=EXPORT_S3_PATH,
            ExportOnly=EXPORT_DB_PATH
        )
        exit(0)


def attach_rds_to_s3_role_to_db(role):
    rds = boto3.client('rds', region_name=REGION_NAME)
    try:
        rds.add_role_to_db_instance(
            DBInstanceIdentifier=DATABASE_ID,
            RoleArn=role.name,
            FeatureName='s3Export'
        )
    except boto_exceptions.ClientError as err:
        msg = 'The engine PostgreSQL supports only one ARN associated with feature name s3Export.'
        if err.response['Error']['Message'] == msg:  # TODO find a more reliable way to check for this
            pass  # ignore if its already added
        else:
            print(err)
            exit(1)

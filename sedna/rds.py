import boto3
import psycopg2
from sedna.common import BUCKET_NAME, REGION_NAME, EXPORT_S3_PATH, DATABASE_ID, \
    SNAPSHOT_ID, EXPORT_DB_PATH, EXPORT_TASK_NAME,  EXPORT_HOST, EXPORT_USER, \
    EXPORT_PASSWORD, EXPORT_DATABASE, PARQUET_PREFIX, SEDNA_ALT_TAGS


def export_views():
    conn = psycopg2.connect(
        host=EXPORT_HOST,
        user=EXPORT_USER,
        password=EXPORT_PASSWORD,
        dbname=EXPORT_DATABASE
    )
    with conn.cursor() as cur:
        cur.execute('CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE;')
        try:
            print(cur.fetchone())  # TODO just need for debug?
        except psycopg2.ProgrammingError:
            pass  # print('No result from CREATE EXTENSION')
        cur.execute(f'''
            SELECT * FROM aws_s3.query_export_to_s3(
                'SELECT * FROM v_internal_generate_allocation_simple_area_table', 
                aws_commons.create_s3_uri(
                    '{BUCKET_NAME}',
                    '{PARQUET_PREFIX}/v_internal_generate_allocation_simple_area_table.csv',
                    '{REGION_NAME}'
                ),
                options :='FORMAT CSV'
            );        
        ''')
        try:
            print(cur.fetchone())  # TODO just need for debug?
        except psycopg2.ProgrammingError:
            print('No result from query_export_to_s3')


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

# TODO will also need to export a CSV of allocation.v_internal_generate_allocation_simple_area_table

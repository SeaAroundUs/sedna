import boto3

S3_BUCKET = 'sedna-catshark-storage'

s3 = boto3.resource('s3')
rds = boto3.client('rds')

# TODO create snapshot for use if doesn't exist?
# TODO create role for use if doesn't exist?

# list snapshots
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Client.describe_db_cluster_snapshots
res = rds.describe_db_cluster_snapshots()
for snapshot in res['DBClusterSnapshots']:
    print(snapshot)

# start export task
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Client.start_export_task
# response = rds.start_export_task(
#     ExportTaskIdentifier='sedna_default_export',
#     SourceArn='TODO',
#     S3BucketName=S3_BUCKET,
#     IamRoleArn='TODO',
#     KmsKeyId='TODO',
#     S3Prefix='TODO',
#     ExportOnly=['seaaroundus']  # TODO might be sau_int on prod
# )

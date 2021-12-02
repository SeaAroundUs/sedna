import boto3
import json
from sedna.common import SEDNA_ALT_TAGS, EXPORT_POLICY_NAME, EXPORT_ROLE_NAME, \
    RDS_TO_S3_POLICY_NAME, RDS_TO_S3_ROLE_NAME, BUCKET_NAME, REGION_NAME, \
    EXPORT_DATABASE, ACCOUNT_ID

EXPORT_POLICY_DOCUMENT = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Action": [
            "s3:PutObject",
            "s3:GetObject",
            "s3:ListBucket",
            "s3:DeleteObject",
            "s3:GetBucketLocation"
        ],
        "Resource": [f"arn:aws:s3:::{BUCKET_NAME}/*"]
    }]
}
EXPORT_ROLE_DOCUMENT = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "export.rds.amazonaws.com"},
        "Action": "sts:AssumeRole"
    }]
}

RDS_TO_S3_POLICY_DOCUMENT = {
    "Version": "2012-10-17",
    "Statement": [{
        "Action": [
            "s3:PutObject",
            "s3:AbortMultipartUpload"
        ],
        "Effect": "Allow",
        "Resource": [f"arn:aws:s3:::{BUCKET_NAME}/*"]
    }]
}
RDS_TO_S3_ROLE_DOCUMENT = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "rds.amazonaws.com"},
        "Action": "sts:AssumeRole",
        "Condition": {
            "StringEquals": {
                "aws:SourceAccount": ACCOUNT_ID,
                "aws:SourceArn": f'arn:aws:rds:{REGION_NAME}:{ACCOUNT_ID}db:{EXPORT_DATABASE}'
            }
        }
    }]
}


def get_or_create_export_policy():
    iam = boto3.client('iam')
    policies = {p['PolicyName']: p['Arn'] for p in iam.list_policies(Scope='Local')['Policies']}
    if EXPORT_POLICY_NAME in policies.keys():
        arn = policies[EXPORT_POLICY_NAME]
    else:
        policy = iam.create_policy(
            PolicyName=EXPORT_POLICY_NAME,
            PolicyDocument=json.dumps(EXPORT_POLICY_DOCUMENT),
            Description='Allows RDS exports to be saved to S3',
            Tags=SEDNA_ALT_TAGS
        )
        arn = policy['Policy']['Arn']
    return boto3.resource('iam').Policy(arn)


def get_or_create_export_role():
    iam = boto3.client('iam')
    roles = {r['RoleName']: r['Arn'] for r in iam.list_roles()['Roles']}
    if EXPORT_ROLE_NAME in roles.keys():
        arn = roles[EXPORT_ROLE_NAME]
    else:
        role = iam.create_role(
            RoleName=EXPORT_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(EXPORT_ROLE_DOCUMENT),
            Description='Role for exporting RDS exports to S3',
            Tags=SEDNA_ALT_TAGS
        )
        arn = role['Role']['Arn']
    return boto3.resource('iam').Role(arn)


def attach_export_policy_to_role(policy):
    iam = boto3.client('iam')
    iam.attach_role_policy(RoleName=EXPORT_ROLE_NAME, PolicyArn=policy.arn)


def get_or_create_rds_to_s3_policy():
    iam = boto3.client('iam')
    policies = {p['PolicyName']: p['Arn'] for p in iam.list_policies(Scope='Local')['Policies']}
    if RDS_TO_S3_POLICY_NAME in policies.keys():
        arn = policies[RDS_TO_S3_POLICY_NAME]
    else:
        policy = iam.create_policy(
            PolicyName=RDS_TO_S3_POLICY_NAME,
            PolicyDocument=json.dumps(RDS_TO_S3_POLICY_DOCUMENT),
            Description='Allows aws_s3 extension to be save to S3',
            Tags=SEDNA_ALT_TAGS
        )
        arn = policy['Policy']['Arn']
    return boto3.resource('iam').Policy(arn)


def get_or_create_rds_to_s3_role():
    iam = boto3.client('iam')
    roles = {r['RoleName']: r['Arn'] for r in iam.list_roles()['Roles']}
    if RDS_TO_S3_ROLE_NAME in roles.keys():
        arn = roles[RDS_TO_S3_ROLE_NAME]
    else:
        role = iam.create_role(
            RoleName=RDS_TO_S3_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(RDS_TO_S3_ROLE_DOCUMENT),
            Description='Role for aws_s3 extension saving to S3',
            Tags=SEDNA_ALT_TAGS
        )
        arn = role['Role']['Arn']
    return boto3.resource('iam').Role(arn)


def attach_rds_to_s3_policy_to_role(policy):
    iam = boto3.client('iam')
    iam.attach_role_policy(RoleName=RDS_TO_S3_ROLE_NAME, PolicyArn=policy.arn)

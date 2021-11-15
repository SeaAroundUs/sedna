import boto3
import json
from sedna.common import SEDNA_TAGS

EXPORT_POLICY_NAME = 'sedna-export-policy'
EXPORT_ROLE_NAME = 'sedna-export-role'
EXPORT_POLICY_DOCUMENT = {
    "Version": "2012-10-17",
    "Statement": [{
        "Sid": "VisualEditor0",
        "Effect": "Allow",
        "Action": [
            "s3:PutObject",
            "s3:GetObject",
            "s3:ListBucket",
            "s3:DeleteObject",
            "s3:GetBucketLocation"
        ],
        "Resource": [
            "arn:aws:s3:::*/*",
            "arn:aws:s3:::sedna-catshark-storage"
        ]
    }]
}
EXPORT_ROLE_DOCUMENT = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {
            "Service": "export.rds.amazonaws.com"
        },
        "Action": "sts:AssumeRole"
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
            Tags=SEDNA_TAGS
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
            Tags=SEDNA_TAGS
        )
        arn = role['Role']['Arn']
    return boto3.resource('iam').Role(arn)


def attach_policy_to_export_role(role, policy):
    iam = boto3.client('iam')
    # TODO boto3 has a bug where role_name is returning arn: https://github.com/boto/boto3/issues/3023
    # TODO check if its already attached? it doesn't error so it might be fine
    iam.attach_role_policy(RoleName=EXPORT_ROLE_NAME, PolicyArn=policy.arn)

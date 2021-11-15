import boto3
import json
from sedna.common import SEDNA_TAGS, REGION_NAME

EXPORT_KEY_NAME = 'sedna-export-key'
KEY_DESCRIPTION = 'Key for encrypting RDS snapshot exports to S3'


def get_or_create_export_key():
    kms = boto3.client('kms', region_name=REGION_NAME)
    for key in kms.list_keys()['Keys']:
        key_meta = kms.describe_key(KeyId=key['KeyId'])['KeyMetadata']
        if key_meta['Description'] == KEY_DESCRIPTION and key_meta['KeyState'] != 'PendingDeletion':
            return key_meta['KeyId']
    kms = boto3.client('kms', region_name=REGION_NAME)
    kms.create_key(
        Policy=generate_key_policy(),
        Description=KEY_DESCRIPTION,
        KeyUsage='ENCRYPT_DECRYPT',
        KeySpec='SYMMETRIC_DEFAULT',
        Origin='AWS_KMS',
        Tags=SEDNA_TAGS,
        MultiRegion=False
    )
    return get_or_create_export_key()


def generate_key_policy():
    return json.dumps({
        "Id": "key-consolepolicy-3",
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "Enable IAM User Permissions",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::689611836922:root"
            },
            "Action": "kms:*",
            "Resource": "*"
        }, {
            "Sid": "Allow access for Key Administrators",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::689611836922:user/sedna-catshark"
            },
            "Action": [
                "kms:Create*",
                "kms:Describe*",
                "kms:Enable*",
                "kms:List*",
                "kms:Put*",
                "kms:Update*",
                "kms:Revoke*",
                "kms:Disable*",
                "kms:Get*",
                "kms:Delete*",
                "kms:TagResource",
                "kms:UntagResource",
                "kms:ScheduleKeyDeletion",
                "kms:CancelKeyDeletion"
            ],
            "Resource": "*"
        }, {
            "Sid": "Allow use of the key",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::689611836922:role/sedna-export-role"
            },
            "Action": [
                "kms:Encrypt",
                "kms:Decrypt",
                "kms:ReEncrypt*",
                "kms:GenerateDataKey*",
                "kms:DescribeKey"
            ],
            "Resource": "*"
        }, {
            "Sid": "Allow attachment of persistent resources",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::689611836922:role/sedna-export-role"
            },
            "Action": [
                "kms:CreateGrant",
                "kms:ListGrants",
                "kms:RevokeGrant"
            ],
            "Resource": "*",
            "Condition": {
                "Bool": {
                    "kms:GrantIsForAWSResource": "true"
                }
            }
        }]
    })

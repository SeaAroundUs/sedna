import botocore.exceptions as boto_exceptions
import boto3


def check_aws_access():
    sts = boto3.client('sts')
    try:
        sts.get_caller_identity()
    except boto_exceptions.ClientError:
        raise Exception(f'Sedna does not have access to AWS; please use the configuration described in README.md')

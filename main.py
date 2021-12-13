import argparse
import sedna.athena as athena
import sedna.iam as iam
import sedna.kms as kms
import sedna.rds as rds


def check_prereqs():
    # TODO check for version
    # TODO check for env
    # TODO check for basic AWS access
    # TODO check for existence of S3
    pass


def permissions():
    # export_policy = iam.get_or_create_export_policy()
    # export_role = iam.get_or_create_export_role()
    # iam.attach_export_policy_to_role(export_policy)
    pass


def snapshot_export():
    # key_id = kms.get_or_create_export_key()
    # snapshot_arn = rds.get_or_create_snapshot()
    # rds.get_or_create_export(snapshot_arn, export_role.name, key_id)
    pass


def view_export():
    # rds_to_s3_policy = iam.get_or_create_rds_to_s3_policy()
    # role = iam.get_or_create_rds_to_s3_role()
    # iam.attach_rds_to_s3_policy_to_role(rds_to_s3_policy)
    # rds.attach_rds_to_s3_role_to_db(role)
    # rds.export_views()
    pass


def test_tables():
    # athena.test_tables()
    pass


def aggregation():
    # TODO
    pass


def allocation():
    # TODO
    pass


def main():
    # TODO handle args with argparse
    check_prereqs()
    permissions()
    snapshot_export()
    view_export()
    test_tables()
    aggregation()
    allocation()


if __name__ == '__main__':
    main()

# import argparse
import sedna.athena as athena
import sedna.iam as iam
import sedna.kms as kms
import sedna.rds as rds


def check_prereqs():
    # TODO check for env settings in file
    # TODO check for export version file
    # TODO check for basic AWS access via boto
    # TODO check for existence of S3 bucket
    # TODO check for RDS access
    pass


def permissions():
    export_policy = iam.get_or_create_export_policy()
    _export_role = iam.get_or_create_export_role()
    iam.attach_export_policy_to_role(export_policy)


def snapshot_export():
    key_id = kms.get_or_create_export_key()
    snapshot_arn = rds.get_or_create_snapshot()
    rds.get_or_create_export(snapshot_arn, export_role.name, key_id)


def view_export():
    rds_to_s3_policy = iam.get_or_create_rds_to_s3_policy()
    role = iam.get_or_create_rds_to_s3_role()
    iam.attach_rds_to_s3_policy_to_role(rds_to_s3_policy)
    rds.attach_rds_to_s3_role_to_db(role)
    rds.export_views()


# none of these tables should require other CTAS tables to run
def setup_athena():
    # athena.create_database()
    # athena.test_tables()
    # athena.create_core_tables()
    athena.create_all_ctas_tables()


def allocation():
    pass  # TODO


def main():
    # TODO handle args with argparse
    # check_prereqs()
    # permissions()
    # snapshot_export()
    # view_export()
    setup_athena()
    allocation()


if __name__ == '__main__':
    main()

# import argparse
import concurrent.futures
import sedna.athena as athena
import sedna.common as common
import sedna.iam as iam
import sedna.kms as kms
import sedna.rds as rds


def check_prereqs():
    # check for env settings in file
    common.check_env_file()
    # TODO check for basic AWS access via boto
    # TODO check for existence of S3 bucket
    # TODO check for RDS access
    pass


def permissions():
    export_policy = iam.get_or_create_export_policy()
    _ = iam.get_or_create_export_role()
    iam.attach_export_policy_to_role(export_policy)


def snapshot_export():
    export_role = iam.get_or_create_export_role()
    key_id = kms.get_or_create_export_key()
    snapshot_arn = rds.get_or_create_snapshot()
    rds.get_or_create_export(snapshot_arn, export_role.name, key_id)


def view_export():
    rds_to_s3_policy = iam.get_or_create_rds_to_s3_policy()
    role = iam.get_or_create_rds_to_s3_role()
    iam.attach_rds_to_s3_policy_to_role(rds_to_s3_policy)
    rds.attach_rds_to_s3_role_to_db(role)
    rds.export_views()


def setup_athena():
    # athena.create_database()
    # athena.create_core_tables()
    # athena.test_tables()
    athena.create_all_ctas_tables()  # TODO run these in a threadpool like allocation


def allocation():
    print('Beginning ALLOCATION!\n---')
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        ids, names = zip(*athena.get_fishing_entities())
        executor.map(athena.allocation_result, ids, names)
    print('ALLOCATION COMPLETE!')


def main():
    # TODO handle args with argparse
    check_prereqs()
    # permissions()
    # snapshot_export()
    # view_export()
    # setup_athena()
    # allocation()


if __name__ == '__main__':
    main()

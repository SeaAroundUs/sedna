import concurrent.futures
import sys

import sedna.athena as athena
import sedna.common as common
import sedna.iam as iam
import sedna.kms as kms
import sedna.rds as rds
import sedna.s3 as s3
import sedna.sts as sts


def check_prereqs():
    print('Beginning configuration check...')
    common.check_env_file()
    sts.check_aws_access()
    s3.check_for_sedna_bucket()
    print('Configuration check passed!')


def permissions():
    print('Beginning permission creation...')
    export_policy = iam.get_or_create_export_policy()
    _ = iam.get_or_create_export_role()
    iam.attach_export_policy_to_role(export_policy)
    print('Permissions created!')


def snapshot_export():
    print('Beginning snapshot export...')
    export_role = iam.get_or_create_export_role()
    key_id = kms.get_or_create_export_key()
    snapshot_arn = rds.get_or_create_snapshot()
    rds.get_or_create_export(snapshot_arn, export_role.name, key_id)
    print('Snapshot exported!')


def view_export():
    print('Beginning view export...')
    rds_to_s3_policy = iam.get_or_create_rds_to_s3_policy()
    role = iam.get_or_create_rds_to_s3_role()
    iam.attach_rds_to_s3_policy_to_role(rds_to_s3_policy)
    rds.attach_rds_to_s3_role_to_db(role)
    rds.export_views()
    print('Views exported!')


def setup_athena():
    print('Beginning Athena setup...')
    athena.create_database()
    athena.create_core_tables()
    athena.test_imported_tables()
    athena.create_all_ctas_tables()  # TODO run these in a threadpool like allocation
    print('Athena setup complete!')


def allocation():
    print('Beginning ALLOCATION!\n---')
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        ids, names = zip(*athena.get_fishing_entities())
        executor.map(athena.allocation_result, ids, names)
    print('ALLOCATION COMPLETE!')


def main():
    if len(sys.argv) == 1:
        print('''
Sedna allocation tool for Sea Around Us

Usage:
    python main.py <command>
    
    Available commands:
    check    - Ensure configuration and proper access
    perm     - Automatically create the proper IAM permissions in AWS
    export   - Export necessary data (views and snapshot) from RDS to S3
    setup    - Set up necessary Athena tables based off S3 data
    allocate - Run the allocation process
''')
        exit(0)

    match sys.argv[1]:
        case 'check':
            check_prereqs()
            exit(0)
        case 'perm':
            permissions()
        case 'export':
            view_export()
            snapshot_export()
            exit(0)
        case 'setup':
            setup_athena()
            exit(0)
        case 'allocate':
            allocation()
            exit(0)
        case _:
            print(f'Unknown command: {sys.argv[1]}')
            exit(1)


if __name__ == '__main__':
    main()

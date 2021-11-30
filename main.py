import sedna.athena as athena
import sedna.iam as iam
import sedna.kms as kms
import sedna.rds as rds


def main():
    policy = iam.get_or_create_export_policy()
    role = iam.get_or_create_export_role()
    iam.attach_policy_to_export_role(role, policy)
    key_id = kms.get_or_create_export_key()
    snapshot_arn = rds.get_or_create_snapshot()
    rds.get_or_create_export(snapshot_arn, role.name, key_id)
    print('export complete! ready for athena work')
    exit(0)
    athena.create_database()
    # athena.create_tables()
    # exit(0)


if __name__ == '__main__':
    main()

import sedna.athena as athena
import sedna.iam as iam
import sedna.kms as kms
import sedna.rds as rds


def main():
    athena.create_database()
    athena.create_tables()
    exit(0)

    policy = iam.get_or_create_export_policy()
    role = iam.get_or_create_export_role()
    iam.attach_policy_to_export_role(role, policy)
    key_id = kms.get_or_create_export_key()
    snapshot_arn = rds.get_or_create_snapshot()
    # TODO wait for snapshot?
    if rds.should_start_export(snapshot_arn):
        print('should start export')
        exit(0)
        # rds.start_export(snapshot_arn, role.name, key_id)
    else:
        print('shouldn\'t start export')
        exit(0)


if __name__ == '__main__':
    main()

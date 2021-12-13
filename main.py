import sedna.athena as athena
import sedna.iam as iam
import sedna.kms as kms
import sedna.rds as rds


def main():
    # prerequisites
    # TODO check for version
    # TODO check for env
    # TODO check for basic AWS access
    # TODO check for existence of S3

    # snapshot and export iam
    export_policy = iam.get_or_create_export_policy()
    export_role = iam.get_or_create_export_role()
    iam.attach_export_policy_to_role(export_policy)

    # snapshot export
    key_id = kms.get_or_create_export_key()
    snapshot_arn = rds.get_or_create_snapshot()
    rds.get_or_create_export(snapshot_arn, export_role.name, key_id)

    # view export
    rds_to_s3_policy = iam.get_or_create_rds_to_s3_policy()
    role = iam.get_or_create_rds_to_s3_role()
    iam.attach_rds_to_s3_policy_to_role(rds_to_s3_policy)
    rds.attach_rds_to_s3_role_to_db(role)
    rds.export_views()

    # athena setup
    athena.create_database()
    athena.create_tables()
    athena.test_tables()
    exit(0)


if __name__ == '__main__':
    main()

import lib.iam as iam


def main():
    policy = iam.get_or_create_export_policy()
    role = iam.get_or_create_export_role()
    iam.attach_policy_to_export_role(role, policy)


if __name__ == '__main__':
    main()

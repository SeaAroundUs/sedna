# Sedna

Story of Sedna [here](https://youtu.be/W3dkfLwc1IE)

Pre-requisites:
- Set up necessary IAM via one of:
  - Environment variables
  - Shared credential file (~/.aws/credentials)
  - AWS config file (~/.aws/config)
- RDS access to the SAU PostgreSQL database
- Ability to create additional IAM roles for Sedna usage

Usage:
```
$ python main.py

Sedna allocation tool for Sea Around Us

Usage:
    python main.py <command>

    Available commands:
    check    - Ensure configuration and proper access
    perm     - Automatically create the proper IAM permissions in AWS
    export   - Export necessary data (views and snapshot) from RDS to S3
    setup    - Set up necessary Athena tables based off S3 data
    allocate - Run the allocation process
```

Check is optional, all other steps must be followed in sequence.
Steps are idempotent so running them more than once is not harmful (and may be required).
Some steps are long-running but nothing should take more than 30 minutes.

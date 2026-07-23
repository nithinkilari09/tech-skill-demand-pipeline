"""
Create (or verify) the AWS IAM role Unity Catalog will assume to reach the raw
S3 bucket, following Databricks' documented two-phase setup exactly:

1. `create`   -- make the role with a placeholder trust policy (external ID "0000")
                 and attach the S3 permissions policy. Do this BEFORE creating the
                 storage credential in Databricks, since Databricks needs a real
                 role ARN to reference.
2. `finalize` -- after creating the storage credential in Databricks (which
                 generates a real external ID), update the trust policy to use
                 that external ID and make the role self-assuming -- Databricks
                 requires this or it rejects the credential as invalid.

Usage:
    python -m scripts.setup_uc_iam_role create
    python -m scripts.setup_uc_iam_role finalize --external-id <id-from-databricks>
"""

import argparse
import json

import boto3
from botocore.exceptions import ClientError

from scripts import config

PERMISSIONS_POLICY_NAME = "tech-skill-demand-pipeline-s3-access"


def placeholder_trust_policy() -> dict:
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": [config.UC_MASTER_ROLE_ARN]},
                "Action": "sts:AssumeRole",
                "Condition": {"StringEquals": {"sts:ExternalId": "0000"}},
            }
        ],
    }


def finalized_trust_policy(external_id: str) -> dict:
    role_arn = f"arn:aws:iam::{config.AWS_ACCOUNT_ID}:role/{config.UC_IAM_ROLE_NAME}"
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                # The role must trust itself (self-assuming) in addition to
                # Databricks' cross-account role, or Databricks rejects the
                # credential as invalid even though AssumeRole would work.
                "Principal": {"AWS": [config.UC_MASTER_ROLE_ARN, role_arn]},
                "Action": "sts:AssumeRole",
                "Condition": {"StringEquals": {"sts:ExternalId": external_id}},
            }
        ],
    }


def permissions_policy() -> dict:
    bucket = config.BUCKET_NAME
    role_arn = f"arn:aws:iam::{config.AWS_ACCOUNT_ID}:role/{config.UC_IAM_ROLE_NAME}"
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket",
                    "s3:GetBucketLocation",
                    "s3:ListBucketMultipartUploads",
                    "s3:ListMultipartUploadParts",
                    "s3:AbortMultipartUpload",
                ],
                "Resource": [f"arn:aws:s3:::{bucket}/*", f"arn:aws:s3:::{bucket}"],
            },
            {
                # Self-assume permission, paired with the self-assuming trust
                # policy statement above -- both sides are required.
                "Effect": "Allow",
                "Action": ["sts:AssumeRole"],
                "Resource": [role_arn],
            },
        ],
    }


def role_exists(iam_client, role_name: str) -> bool:
    try:
        iam_client.get_role(RoleName=role_name)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchEntity":
            return False
        raise


def cmd_create(iam_client):
    role_name = config.UC_IAM_ROLE_NAME
    trust_policy_json = json.dumps(placeholder_trust_policy())

    if role_exists(iam_client, role_name):
        print(f"Role already exists: {role_name} -- updating trust policy to placeholder state.")
        iam_client.update_assume_role_policy(RoleName=role_name, PolicyDocument=trust_policy_json)
        role = iam_client.get_role(RoleName=role_name)["Role"]
    else:
        role = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=trust_policy_json,
            Description="Unity Catalog storage credential role for tech-skill-demand-pipeline raw S3 bucket",
        )["Role"]
        print(f"Created role: {role_name}")

    iam_client.put_role_policy(
        RoleName=role_name,
        PolicyName=PERMISSIONS_POLICY_NAME,
        PolicyDocument=json.dumps(permissions_policy()),
    )
    print(f"Attached inline policy: {PERMISSIONS_POLICY_NAME}")

    print(f"\nRole ARN (use this when creating the storage credential in Databricks):\n  {role['Arn']}")
    print("\nNext: create the storage credential in Databricks with this Role ARN, "
          "copy the external ID it generates, then run:\n"
          "  python -m scripts.setup_uc_iam_role finalize --external-id <id>")


def cmd_finalize(iam_client, external_id: str):
    role_name = config.UC_IAM_ROLE_NAME
    trust_policy_json = json.dumps(finalized_trust_policy(external_id))
    iam_client.update_assume_role_policy(RoleName=role_name, PolicyDocument=trust_policy_json)
    print(f"Trust policy finalized on {role_name} with external ID {external_id} (self-assuming).")
    print("\nNext: validate the storage credential in Databricks, then create the external location.")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("create")
    finalize_parser = sub.add_parser("finalize")
    finalize_parser.add_argument("--external-id", required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    iam = boto3.client("iam")

    if args.action == "create":
        cmd_create(iam)
    elif args.action == "finalize":
        cmd_finalize(iam, args.external_id)


if __name__ == "__main__":
    main()

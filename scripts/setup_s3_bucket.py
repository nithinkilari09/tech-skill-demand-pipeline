"""
Create (or verify) the raw-landing S3 bucket with the exact configuration this
project needs: us-east-1, all public access blocked, versioning off, and a
lifecycle rule expiring objects after ~90 days.

Idempotent -- safe to re-run. If the bucket already exists (created by a
previous run), it just re-applies the public-access-block and lifecycle
settings rather than failing.

Usage:
    python -m scripts.setup_s3_bucket
"""

import boto3
from botocore.exceptions import ClientError

from scripts import config


def bucket_exists(s3_client, bucket_name: str) -> bool:
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
            return False
        raise


def create_bucket(s3_client, bucket_name: str, region: str):
    # us-east-1 is the SDK's implicit default region -- passing a
    # CreateBucketConfiguration/LocationConstraint for it is actually rejected
    # by the API ("InvalidLocationConstraint"), so it has to be the one region
    # we *don't* pass a location constraint for.
    if region == "us-east-1":
        s3_client.create_bucket(Bucket=bucket_name)
    else:
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": region},
        )


def apply_public_access_block(s3_client, bucket_name: str):
    s3_client.put_public_access_block(
        Bucket=bucket_name,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )


def apply_lifecycle_rule(s3_client, bucket_name: str, expiration_days: int, prefix: str):
    s3_client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name,
        LifecycleConfiguration={
            "Rules": [
                {
                    "ID": "expire-raw-postings",
                    "Status": "Enabled",
                    # Scoped to the raw-landing prefix only -- NOT the whole bucket --
                    # since Unity Catalog managed Delta tables live in this same
                    # bucket under warehouse/ and must not expire.
                    "Filter": {"Prefix": prefix},
                    "Expiration": {"Days": expiration_days},
                }
            ]
        },
    )


def main():
    s3 = boto3.client("s3", region_name=config.AWS_REGION)

    if bucket_exists(s3, config.BUCKET_NAME):
        print(f"Bucket already exists: {config.BUCKET_NAME} -- re-applying config.")
    else:
        create_bucket(s3, config.BUCKET_NAME, config.AWS_REGION)
        print(f"Created bucket: {config.BUCKET_NAME} in {config.AWS_REGION}")

    # Versioning is intentionally left alone: a brand-new bucket defaults to
    # versioning "off" (unconfigured), which is exactly the "off" state we want --
    # calling put_bucket_versioning would move it to the "Suspended" state instead,
    # which is a different (if functionally similar) status for no real benefit.

    apply_public_access_block(s3, config.BUCKET_NAME)
    print("Applied: block all public access")

    apply_lifecycle_rule(s3, config.BUCKET_NAME, config.LIFECYCLE_EXPIRATION_DAYS, config.RAW_LIFECYCLE_PREFIX)
    print(f"Applied: lifecycle rule expiring '{config.RAW_LIFECYCLE_PREFIX}*' objects "
          f"after {config.LIFECYCLE_EXPIRATION_DAYS} days")

    print(f"\nDone. Bucket ready: s3://{config.BUCKET_NAME}")


if __name__ == "__main__":
    main()

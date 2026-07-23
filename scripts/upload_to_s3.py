"""
Upload the locally-landed raw JSONL partitions (data/raw/source=X/ingestion_date=Y/...)
to the S3 bucket, preserving the exact same key structure so Bronze can read
partitions directly off S3 the same way it would off local disk.

Usage:
    python -m scripts.upload_to_s3
"""

from pathlib import Path

import boto3

from scripts import config

REPO_ROOT = Path(__file__).resolve().parent.parent
LOCAL_RAW_DIR = REPO_ROOT / config.LOCAL_RAW_DIR_NAME


def iter_local_files(local_dir: Path):
    for path in sorted(local_dir.rglob("*.jsonl")):
        key = path.relative_to(local_dir).as_posix()
        yield path, key


def main():
    if not LOCAL_RAW_DIR.exists():
        print(f"Nothing to upload -- {LOCAL_RAW_DIR} does not exist. "
              f"Run `python -m ingestion.pool_postings` first.")
        return

    s3 = boto3.client("s3", region_name=config.AWS_REGION)

    uploaded = 0
    for local_path, key in iter_local_files(LOCAL_RAW_DIR):
        # Key is just source=X/ingestion_date=Y/part-NNN.jsonl -- the bucket
        # itself is the raw landing zone, so the local data/raw/ path prefix
        # doesn't belong in the S3 key.
        s3.upload_file(str(local_path), config.BUCKET_NAME, key)
        print(f"  uploaded -> s3://{config.BUCKET_NAME}/{key}")
        uploaded += 1

    print(f"\nDone. {uploaded} file(s) uploaded to s3://{config.BUCKET_NAME}/")


if __name__ == "__main__":
    main()

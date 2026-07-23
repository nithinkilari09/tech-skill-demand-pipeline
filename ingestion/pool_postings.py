"""
Pool live job postings from RemoteOK and Arbeitnow and land them as raw,
ingestion-date-partitioned JSON Lines -- the Bronze "raw as landed" input.

No filtering, no dedup, no skill extraction happens here -- that's Silver's job,
done later in Databricks/PySpark. This script's only responsibility is: fetch
what the APIs return today, tag it with when/where it came from, write it out.

Usage:
    python -m ingestion.pool_postings                    # land locally under data/raw/
    python -m ingestion.pool_postings --output-dir data/raw

Output:
    data/raw/source=remoteok/ingestion_date=YYYY-MM-DD/part-000.jsonl
    data/raw/source=arbeitnow/ingestion_date=YYYY-MM-DD/part-000.jsonl ... part-00N.jsonl
"""

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from ingestion import config

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_DIR = REPO_ROOT / "data" / "raw"


def _get_with_retries(url: str, params: dict | None = None) -> requests.Response:
    headers = {"User-Agent": config.USER_AGENT}
    last_exc = None
    for attempt in range(1, config.REQUEST_RETRIES + 1):
        try:
            resp = requests.get(url, headers=headers, params=params,
                                 timeout=config.REQUEST_TIMEOUT_SECONDS)
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            last_exc = exc
            if attempt < config.REQUEST_RETRIES:
                time.sleep(config.RETRY_BACKOFF_SECONDS * attempt)
    raise RuntimeError(f"GET {url} failed after {config.REQUEST_RETRIES} attempts") from last_exc


def fetch_remoteok(ingested_at: str) -> list[list[dict]]:
    """Returns a list with one 'shard' (RemoteOK has no pagination on this endpoint)."""
    resp = _get_with_retries(config.REMOTEOK_URL)
    raw = resp.json()

    # Element 0 is always RemoteOK's legal/attribution notice, not a posting.
    postings = [p for p in raw if "id" in p]

    for p in postings:
        p["_source"] = "remoteok"
        p["_ingested_at"] = ingested_at

    return [postings]


def fetch_arbeitnow(ingested_at: str) -> list[list[dict]]:
    """Returns one 'shard' per page fetched, mirroring the real paginated batches."""
    shards = []
    page = 1
    while page <= config.ARBEITNOW_MAX_PAGES:
        resp = _get_with_retries(config.ARBEITNOW_URL, params={"page": page})
        body = resp.json()
        postings = body.get("data", [])
        if not postings:
            break

        for p in postings:
            p["_source"] = "arbeitnow"
            p["_ingested_at"] = ingested_at

        shards.append(postings)
        page += 1
        time.sleep(config.ARBEITNOW_PAGE_DELAY_SECONDS)

    return shards


def write_shards(output_dir: Path, source: str, ingestion_date: str, shards: list[list[dict]]) -> Path:
    partition_dir = output_dir / f"source={source}" / f"ingestion_date={ingestion_date}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    for i, shard in enumerate(shards):
        part_file = partition_dir / f"part-{i:03d}.jsonl"
        with open(part_file, "w", encoding="utf-8") as f:
            for record in shard:
                f.write(json.dumps(record) + "\n")
    return partition_dir


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output-dir", type=str, default=str(DEFAULT_OUTPUT_DIR),
                        help=f"Root directory to land partitioned JSONL files (default: {DEFAULT_OUTPUT_DIR})")
    return parser.parse_args()


def main():
    args = parse_args()
    output_dir = Path(args.output_dir)
    now = datetime.now(timezone.utc)
    ingestion_date = now.strftime("%Y-%m-%d")
    ingested_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    print(f"Pooling postings for ingestion_date={ingestion_date} ...")

    remoteok_shards = fetch_remoteok(ingested_at)
    remoteok_count = sum(len(s) for s in remoteok_shards)
    remoteok_dir = write_shards(output_dir, "remoteok", ingestion_date, remoteok_shards)
    print(f"  remoteok:  {remoteok_count:>5,} postings, {len(remoteok_shards)} shard(s) -> {remoteok_dir}")

    arbeitnow_shards = fetch_arbeitnow(ingested_at)
    arbeitnow_count = sum(len(s) for s in arbeitnow_shards)
    arbeitnow_dir = write_shards(output_dir, "arbeitnow", ingestion_date, arbeitnow_shards)
    print(f"  arbeitnow: {arbeitnow_count:>5,} postings, {len(arbeitnow_shards)} shard(s) -> {arbeitnow_dir}")

    print(f"\nDone. {remoteok_count + arbeitnow_count:,} total postings landed.")


if __name__ == "__main__":
    main()

"""Shared config for S3 bucket setup + upload scripts."""

# Bucket names are globally unique across all of AWS, not just this account --
# suffixing with the account ID guarantees no collision without needing a registry.
AWS_ACCOUNT_ID = "039624954996"
BUCKET_NAME = f"tech-skill-demand-pipeline-raw-{AWS_ACCOUNT_ID}"
AWS_REGION = "us-east-1"

# Raw postings have no long-term value once Bronze/Silver have processed them --
# expire objects out of the raw landing zone after ~90 days to bound storage cost.
# Scoped to the source=.../ prefix specifically (not the whole bucket) because the
# same bucket also holds the Unity Catalog managed-table storage under warehouse/ --
# Delta table data must NOT expire the way raw JSON landing does.
LIFECYCLE_EXPIRATION_DAYS = 90
RAW_LIFECYCLE_PREFIX = "source="

LOCAL_RAW_DIR_NAME = "data/raw"

# Unity Catalog managed storage (Bronze/Silver/Gold Delta tables) lives under this
# prefix in the SAME bucket as raw landing -- deliberately outside the lifecycle
# rule's prefix filter above, and covered by the same external location since it
# grants access to the whole bucket.
WAREHOUSE_PREFIX = "warehouse"

# --- Unity Catalog storage credential (AWS IAM role) ---
# Databricks' own cross-account role for Unity Catalog on standard (non-GovCloud) AWS --
# published in their docs, not something we generate ourselves.
UC_MASTER_ROLE_ARN = "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"

UC_IAM_ROLE_NAME = "tech-skill-demand-pipeline-uc-role"
UC_STORAGE_CREDENTIAL_NAME = "tech_skill_demand_pipeline_raw_cred"
UC_EXTERNAL_LOCATION_NAME = "tech_skill_demand_pipeline_raw_loc"

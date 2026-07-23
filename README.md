# Tech Skill-Demand Analytics Pipeline

A real-time analytics pipeline over live tech job postings: raw postings land in S3,
get refined through a Bronze/Silver/Gold medallion architecture on Databricks (PySpark +
Delta Lake + Unity Catalog), and surface as a static, interactive dashboard published to
GitHub Pages — refreshed on a schedule with no live backend to keep running.

**Business narrative:** real recruiter/employer tool demand across CS domains — which
skills and tools (Python, SQL, React, AWS, Docker, ...) show up most often in postings
for data engineers, data analysts, frontend developers, full-stack developers, and other
CS roles, and how that shifts over time. Deliberately not framed around employment status
or visa sponsorship.

## Status

🟢 **Silver layer complete and hand-verified** — 1,002 postings deduped on
`(source, job_id)`, classified into a fixed domain taxonomy, and matched against a
51-entry skill dictionary into a posting-to-skill fact table (805 rows). Skill-extraction
output was hand-checked against real postings, which caught and fixed three real bugs
(a Unicode-boundary regex bug, undecoded HTML entities, and English-word ambiguity on
"Go") — full detail in BUILD_LOG.md.

Milestones (each confirmed with the project owner before moving to the next):
- [x] RemoteOK + Arbeitnow ingestion script (pooling, tested against live APIs)
- [x] S3 landing zone working (bucket + real upload test)
- [x] Unity Catalog storage credential + external location verified working
- [x] Bronze layer reading real data into Delta tables
- [x] Silver layer: dedup, domain classification, skill extraction (hand-verified)
- [ ] Gold layer (skill-demand-by-domain aggregates) queryable from the Databricks SQL warehouse
- [ ] GitHub Pages dashboard live (Plotly, static HTML, via `databricks-sql-connector`)
- [ ] Databricks Workflows DAG scheduled (`Trigger.AvailableNow`)

## Architecture

```
┌───────────────────┐     ┌──────────────────┐     ┌──────────────────────────────────┐
│ ingestion/          │───▶│  AWS S3           │───▶│  Databricks + PySpark              │
│ pool_postings.py    │    │  (raw landing,    │    │  Unity Catalog: storage credential  │
│ RemoteOK + Arbeitnow│    │   us-east-1,      │    │  + external location → S3 bucket    │
│ scheduled daily via │    │   block public    │    │                                     │
│ GitHub Actions       │    │   access, no       │    │  Bronze  → raw postings as landed   │
│ (cron)               │    │   versioning,      │    │  Silver  → cleaned/deduplicated,     │
└───────────────────┘     │   ~90-day lifecycle│    │            skills extracted via a      │
                            │   source=X/         │    │            maintained keyword dict     │
                            │   ingestion_date=Y/ │    │  Gold    → skill-demand-by-domain       │
                            └──────────────────┘    │            aggregates over time,          │
                                                       │            all as Delta tables            │
                                                       │                                            │
                                                       │  Orchestration: Databricks Workflows,       │
                                                       │  Trigger.AvailableNow (only trigger          │
                                                       │  serverless compute supports) — scheduled     │
                                                       │  incremental runs, not 24/7                    │
                                                       └──────────────────┬─────────────────────────────┘
                                                                          │
                                                                          │ Gold Delta tables served directly
                                                                          │ from Databricks' built-in SQL
                                                                          │ warehouse (Free Edition, 2X-Small)
                                                                          ▼
                                                              ┌────────────────────────────┐
                                                              │  GitHub Actions (cron)       │
                                                              │  queries the SQL warehouse    │
                                                              │  via databricks-sql-connector │
                                                              │  → renders Plotly HTML        │
                                                              │  → publishes to GitHub Pages   │
                                                              └────────────────────────────┘
```

**No Snowflake, no dbt** in this project — both intentionally reserved for a different
project elsewhere in this portfolio, so tooling doesn't overlap across projects. Gold
aggregates are served straight from Databricks' own SQL warehouse instead of a separate
data warehouse.

**Why Unity Catalog storage credential + external location, not notebooks reading S3
directly.** This is how Databricks Free Edition is meant to reach external cloud storage
without embedding raw AWS keys in notebook code — a storage credential (IAM role or
access-key-based) plus an external location object let Bronze notebooks reference the S3
path through Unity Catalog's governance layer instead of ad hoc `boto3`/`s3a://` config
scattered across notebooks. Concretely: IAM role `tech-skill-demand-pipeline-uc-role`
(self-assuming, trusts Databricks' cross-account role with a generated external ID) →
storage credential `tech_skill_demand_pipeline_raw_cred` → external location
`tech_skill_demand_pipeline_raw_loc` pointing at the bucket root. Both the raw JSONL
landing (`source=X/ingestion_date=Y/...`) and the Unity Catalog managed Delta table
storage (`warehouse/...`, catalog `tech_skill_demand`, schemas `bronze`/`silver`/`gold`)
live in the *same* bucket, covered by this one external location — but the bucket's
90-day lifecycle rule is scoped to the `source=` prefix only, so Delta table data under
`warehouse/` never expires the way raw JSON landing does.

**Why Bronze uses Auto Loader (`cloudFiles`) with `trigger(availableNow=True)` instead of
a plain batch read.** This is the same trigger the Workflows orchestration milestone will
schedule daily, so building Bronze this way now means wiring up orchestration later is
"point a schedule at this," not "rewrite Bronze as streaming." One notebook
(`notebooks/bronze_ingest.py`), parameterized by a `source` widget rather than a unified
table, because RemoteOK and Arbeitnow schemas are genuinely incompatible
(`position`/`company` vs. `title`/`company_name`) — merging them at Bronze would force
normalization that belongs in Silver.

**Why `Trigger.AvailableNow` instead of a continuous streaming trigger.** It's the only
trigger type Databricks Free Edition's serverless compute supports for what would
otherwise be a streaming read — it processes everything currently available and then
stops, which is exactly "run once per scheduled Workflow trigger," not "run forever."
That keeps the pipeline squarely inside free-tier compute instead of paying for an
always-on cluster.

**Why static Plotly HTML instead of Streamlit/a live app.** Free-tier live-server hosts
(Streamlit Community Cloud, HF Spaces, etc.) sleep after inactivity, so first load after
idle time is slow or broken. Pre-rendered Plotly HTML has zero backend, never sleeps,
loads instantly, is free forever on GitHub Pages, and still keeps real interactivity
(hover, zoom, legend toggle, filter dropdowns) because that logic is embedded client-side
in the HTML/JS Plotly generates.

## Repo layout

```
tech-skill-demand-pipeline/
├── BUILD_LOG.md            # engineering journal — reasoning, decisions, what broke, written as-we-go
├── README.md                # you are here
├── requirements.txt
├── ingestion/                 # pools RemoteOK + Arbeitnow, lands raw partitioned JSONL
│   ├── config.py                # endpoint URLs, User-Agent, retry/backoff, politeness delay
│   └── pool_postings.py         # fetch + tag with _source/_ingested_at + write, no transformation
├── data/raw/                   # gitignored — source=X/ingestion_date=YYYY-MM-DD/part-NNN.jsonl
├── scripts/                     # S3 + Unity Catalog setup
│   ├── config.py                  # bucket name, region, lifecycle prefix/days, UC role/credential/location names
│   ├── setup_s3_bucket.py         # idempotent bucket create + public-access-block + scoped lifecycle rule
│   ├── upload_to_s3.py            # uploads data/raw/ to S3, preserving partition keys
│   └── setup_uc_iam_role.py       # IAM role create (placeholder trust policy) + finalize (real external ID)
├── notebooks/                   # Databricks Bronze/Silver/Gold notebooks (PySpark, Delta Lake)
│   ├── bronze_ingest.py            # Auto Loader, trigger(availableNow=True), parameterized by source
│   └── silver_transform.py         # normalize, dedup, domain classification, skill extraction
└── dashboard/                    # (later) Plotly HTML + GitHub Pages site
```

## Data sources

| source     | auth needed | endpoint                                            | notes |
|------------|-------------|------------------------------------------------------|-------|
| RemoteOK   | none        | `GET https://remoteok.com/api`                        | ~100 most recent listings per call, no pagination. Element `[0]` is a legal/attribution notice, filtered out. **ToS requires linking back to RemoteOK** wherever the data is displayed — dashboard footer must credit them. |
| Arbeitnow  | none        | `GET https://www.arbeitnow.com/api/job-board-api?page=N` | 100 postings/page, ~900 total live postings as of testing (2026-07-22). EU/Germany-focused. Docs ask (not require) a link back and reasonable request rates. |
| Jooble     | API key     | optional bonus source, not yet integrated              | only added if free access proves reliable |
| The Muse   | none (rate-limited) | optional bonus source, not yet integrated       | only added if free access proves reliable |

Both required sources were verified live (not from documentation alone — several
secondhand sources online were outdated/conflicting) before writing any ingestion code.
No registration, no API key, no account needed for RemoteOK or Arbeitnow.

## Landed schema (Bronze — raw as landed, no transformation)

Every record from both sources gets two fields added at ingestion time, on top of
whatever fields that source's API returns natively:

| field          | notes                                                              |
|----------------|-----------------------------------------------------------------------|
| `_source`      | `remoteok` \| `arbeitnow`                                              |
| `_ingested_at` | UTC timestamp the record was pulled, `YYYY-MM-DDTHH:MM:SSZ`             |

Everything else is passed through unmodified from the source API (title/position,
company, description, tags, location, URL, timestamps, etc. — the two sources don't
share a schema, which is expected and exactly why cleaning/normalizing happens in
Silver, not here).

Skill extraction (matching description/title text against a maintained keyword
dictionary) and domain classification (data engineer / data analyst / frontend /
full-stack / other) both happen in Silver, in PySpark — not at ingestion — since neither
source's own tags are clean or consistent enough to trust directly (confirmed while
testing: Arbeitnow's tags include things like "Management" and "Automotive Engineering"
alongside real tech skills).

## Silver layer

Three Delta tables in `tech_skill_demand.silver`:

| table                | grain                    | notes |
|----------------------|--------------------------|-------|
| `cleaned_postings`   | one row per `(source, job_id)` | normalized common schema; keeps *first-seen* content, tracks `first_seen_ingestion_date`/`last_seen_ingestion_date`/`times_seen` separately (a posting recurring across days is a real "still active" signal, not noise) |
| `posting_skills`     | one row per `(source, job_id, skill)` | fact table — a posting mentioning 5 tools produces 5 rows |
| `skill_dictionary`   | one row per skill        | the maintained keyword dictionary itself, as queryable data (skill → category), not just code |

**Domain classification** is rule-based (regex over title, then tags as fallback), a
fixed taxonomy: `data engineer`, `data analyst`, `frontend`, `full-stack`, `backend`,
`mobile`, `other/uncategorized`. Titles that don't clearly match land in
`other/uncategorized` on purpose — forcing a weak match would be worse than an honest
"doesn't fit" bucket. **Known limitation:** many Arbeitnow postings have German-language
titles this English-only rule set can't match, systematically under-representing them in
the domain breakdown.

**Skill extraction** matches a 51-entry hand-curated dictionary (languages, cloud, data
tools, databases, BI tools, frameworks, devops, ML) case-insensitively with word
boundaries — except `R`, `C`, and `Go`, which get case-sensitive, context-aware patterns.
These three went through several rounds of hand-verification against real postings before
landing on patterns that held up — see BUILD_LOG.md's 2026-07-23 Silver entry for the
full investigation (a Unicode-boundary regex bug, undecoded HTML entities, and "Go"
requiring co-occurrence with another recognized language to count, since it's an ordinary
English word otherwise). The dictionary is explicitly a maintained list, not exhaustive —
extend `SKILL_DICTIONARY` in `notebooks/silver_transform.py` as new tools show up.

## Setup

```bash
# from the repo root
pip install -r requirements.txt

# AWS credentials must already be configured (~/.aws/credentials + ~/.aws/config,
# or environment variables) with permissions on the S3 bucket below.

# pool today's live postings from RemoteOK + Arbeitnow, land locally
python -m ingestion.pool_postings

# one-time (or re-run anytime — idempotent): create/verify the S3 bucket
python -m scripts.setup_s3_bucket

# upload the locally-landed partitions to S3
python -m scripts.upload_to_s3
```

S3 bucket: `tech-skill-demand-pipeline-raw-039624954996` (us-east-1). Config lives in
`scripts/config.py`.

```bash
# Unity Catalog IAM role -- two-phase, see scripts/setup_uc_iam_role.py docstring
python -m scripts.setup_uc_iam_role create
# ... then create the storage credential in Databricks with the printed Role ARN,
#     copy the external ID it generates ...
python -m scripts.setup_uc_iam_role finalize --external-id <id-from-databricks>
```

Requires a `~/.databrickscfg` with `host` + a scoped personal access token
(`unity-catalog`, `workspace`, `jobs`, `sql`, `clusters`, `identity` scopes) for
`databricks-sdk` to authenticate.

Unity Catalog: catalog `tech_skill_demand`, schemas `bronze`/`silver`/`gold`, storage
credential `tech_skill_demand_pipeline_raw_cred`, external location
`tech_skill_demand_pipeline_raw_loc`.

## Known constraints to plan around

- **Snowflake / dbt** are explicitly out of scope for this project (reserved for a
  different project in this portfolio) — no trial-clock timing concern here.
- The AWS IAM user (`tech-skill-pipeline-uploader`) holds `AmazonS3FullAccess` plus a
  narrow inline policy scoped to managing exactly one IAM role
  (`tech-skill-demand-pipeline-uc-role`) — not broader IAM access.
- The Databricks personal access token has a fixed lifetime (90 days from generation) —
  will need regenerating if the project outlives that.

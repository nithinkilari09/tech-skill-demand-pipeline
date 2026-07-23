"""
Configuration for the job-posting ingestion pool.

Both RemoteOK and Arbeitnow are fully public APIs -- no API key, no account,
no OAuth. Confirmed live on 2026-07-22:
  - RemoteOK  (https://remoteok.com/api)              -> GET, no auth, returns
    the current ~100 most recent listings as a JSON array (element 0 is a
    "legal"/attribution notice, not a job -- must be filtered out).
  - Arbeitnow (https://www.arbeitnow.com/api/job-board-api) -> GET, no auth,
    paginated via ?page=N, 100 jobs/page, empty `data` once past the last page.

Both APIs' terms ask (RemoteOK requires, Arbeitnow requests) attribution: a
followed link back to the source site from anywhere their data is displayed.
That link lives in the dashboard footer (added when dashboard/ is built) --
see BUILD_LOG.md for the 2026-07-22 entry.
"""

REMOTEOK_URL = "https://remoteok.com/api"
ARBEITNOW_URL = "https://www.arbeitnow.com/api/job-board-api"

# Identify the script honestly and point back at the repo, per API good-citizenship
# norms (RemoteOK's ToS is explicit about wanting attribution / a real UA).
USER_AGENT = (
    "tech-skill-demand-pipeline/0.1 "
    "(+https://github.com/REPLACE_WITH_GH_USERNAME/tech-skill-demand-pipeline; "
    "portfolio data-engineering project)"
)

REQUEST_TIMEOUT_SECONDS = 15
REQUEST_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2

# Politeness delay between paginated Arbeitnow requests. Arbeitnow's own docs
# just say "please do not abuse" with no published rate limit -- this is us
# self-imposing a sane floor rather than firing ~10 requests back-to-back.
ARBEITNOW_PAGE_DELAY_SECONDS = 1.0
ARBEITNOW_MAX_PAGES = 50  # circuit breaker in case pagination never terminates

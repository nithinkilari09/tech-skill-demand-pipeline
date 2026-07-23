# Databricks notebook source
# Silver: normalize RemoteOK + Arbeitnow into one schema, dedup on (source, job_id),
# extract mentioned skills/tools, then classify each posting into a fixed domain
# taxonomy using BOTH title and the skills just extracted.
#
# Design decisions (see BUILD_LOG.md for full reasoning):
# - Dedup keeps the FIRST-seen row's content per (source, job_id), but separately
#   tracks first_seen_ingestion_date / last_seen_ingestion_date / times_seen --
#   a posting still appearing on day N is a real "still active" signal, not noise
#   to collapse away.
# - Skill extraction runs BEFORE domain classification (not after) specifically so
#   classification can use skills as a second signal, not just title/tags -- a
#   posting titled just "Software Engineer" that mentions dbt/Airflow/Spark is a
#   real data-engineering signal even though the title alone says nothing.
# - Domain classification is a fixed, rule-based (regex + skill-signal) taxonomy,
#   not ML. Titles/skills that don't clearly match stay in "other/uncategorized"
#   rather than being forced into a weak match.
# - Skill extraction produces one row per (source, job_id, skill) -- a posting
#   mentions many tools. Matching is case-insensitive word-boundary EXCEPT for a
#   few short/ambiguous language names (R, C, Go) which get case-sensitive,
#   context-aware patterns to avoid false positives ("R&D", "HR", "let's go").
# - The skill dictionary is a maintained, hand-curated list -- broad, not
#   exhaustive. New tools get added over time; this is a known limitation.

# COMMAND ----------

import re
import html as _html
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.window import Window

CATALOG = "tech_skill_demand"

# COMMAND ----------

# MAGIC %md ### Normalize Bronze -> common schema

# COMMAND ----------

remoteok = spark.table(f"{CATALOG}.bronze.remoteok_postings").select(
    F.lit("remoteok").alias("source"),
    F.col("id").alias("job_id"),
    F.col("position").alias("title"),
    F.col("company").alias("company"),
    F.col("location").alias("location"),
    F.coalesce("apply_url", "url").alias("url"),
    F.col("description").alias("description"),
    F.from_json("tags", ArrayType(StringType())).alias("tags"),
    F.lit(True).alias("remote"),  # RemoteOK is a remote-only job board
    F.to_timestamp(F.from_unixtime(F.col("epoch").cast("long"))).alias("posted_at"),
    F.col("ingestion_date").alias("ingestion_date"),
    F.col("_bronze_ingested_at").alias("_bronze_ingested_at"),
)

arbeitnow = spark.table(f"{CATALOG}.bronze.arbeitnow_postings").select(
    F.lit("arbeitnow").alias("source"),
    F.col("slug").alias("job_id"),
    F.col("title").alias("title"),
    F.col("company_name").alias("company"),
    F.col("location").alias("location"),
    F.col("url").alias("url"),
    F.col("description").alias("description"),
    F.from_json("tags", ArrayType(StringType())).alias("tags"),
    (F.col("remote") == "true").alias("remote"),
    F.to_timestamp(F.from_unixtime(F.col("created_at").cast("long"))).alias("posted_at"),
    F.col("ingestion_date").alias("ingestion_date"),
    F.col("_bronze_ingested_at").alias("_bronze_ingested_at"),
)

normalized = remoteok.unionByName(arbeitnow)

# Strip HTML tags AND decode HTML entities for a clean text column used in
# skill/domain matching -- raw `description` is left untouched (Silver cleans,
# doesn't destroy source data). Entity decoding matters more than it looks:
# undecoded "&#x26;" (a literal "&") was splitting ampersand-abbreviation names
# like "C&A" (a retailer) and "P&C Mentor" into a bare "C" that then looked like
# a standalone token to the skill matcher -- found by hand-checking real
# postings, see BUILD_LOG.md.
_unescape_udf = F.udf(lambda s: _html.unescape(s) if s else s, StringType())

normalized = normalized.withColumn(
    "description_clean",
    _unescape_udf(F.trim(F.regexp_replace(F.col("description"), r"<[^>]+>", " "))),
)

# COMMAND ----------

# MAGIC %md ### Dedup on (source, job_id): keep first-seen content, track recurrence
# MAGIC
# MAGIC Full recompute from all of Bronze each run (not an incremental MERGE) --
# MAGIC deliberate choice at this data scale (low thousands of rows): simpler, and
# MAGIC fully correct every run since first/last/times_seen are recomputed
# MAGIC deterministically from complete history rather than patched incrementally.
# MAGIC Revisit as a MERGE if/when Bronze history grows large enough that a full
# MAGIC rescan becomes costly.

# COMMAND ----------

first_seen_window = Window.partitionBy("source", "job_id").orderBy(
    F.col("ingestion_date").asc(), F.col("_bronze_ingested_at").asc()
)
ranked = normalized.withColumn("rn", F.row_number().over(first_seen_window))
first_seen_rows = ranked.filter(F.col("rn") == 1).drop("rn", "ingestion_date", "_bronze_ingested_at")

recurrence = normalized.groupBy("source", "job_id").agg(
    F.min("ingestion_date").alias("first_seen_ingestion_date"),
    F.max("ingestion_date").alias("last_seen_ingestion_date"),
    F.countDistinct("ingestion_date").alias("times_seen"),
)

deduped = first_seen_rows.join(recurrence, on=["source", "job_id"], how="inner")

# COMMAND ----------

# MAGIC %md ### Skill/tool extraction (computed BEFORE domain classification)
# MAGIC
# MAGIC One row per (source, job_id, skill) ends up in the fact table below. Matched
# MAGIC against title + cleaned description + tags, case-insensitive with word
# MAGIC boundaries -- except R, C, and Go, which get case-sensitive, context-aware
# MAGIC patterns since they're common English words/substrings otherwise (see
# MAGIC BUILD_LOG.md for the false-positive cases this avoids).
# MAGIC
# MAGIC This dictionary is a maintained, hand-curated list -- broad coverage
# MAGIC (languages, cloud, data tools, databases, BI tools, frameworks, devops, ML,
# MAGIC web tech) but explicitly NOT exhaustive.

# COMMAND ----------

SKILL_DICTIONARY = [
    # languages
    {"skill": "Python", "category": "language", "patterns": [r"\bpython\b"]},
    {"skill": "Java", "category": "language", "patterns": [r"\bjava\b(?!script)"]},
    {"skill": "JavaScript", "category": "language", "patterns": [r"\bjavascript\b", r"\bjs\b"]},
    {"skill": "TypeScript", "category": "language", "patterns": [r"\btypescript\b"]},
    {"skill": "SQL", "category": "language", "patterns": [r"\bsql\b"]},
    # "Go" still gets a dictionary entry (so it shows up in the reference
    # table below) but no patterns here -- matching is fully special-cased in
    # extract_skills(), see the comment there for why.
    {"skill": "Go", "category": "language"},
    {"skill": "Rust", "category": "language", "patterns": [r"\brust\b"]},
    {"skill": "C++", "category": "language", "case_sensitive_patterns": [r"C\+\+"]},
    {"skill": "C#", "category": "language", "case_sensitive_patterns": [r"C#"]},
    {"skill": "PHP", "category": "language", "patterns": [r"\bphp\b"]},
    {"skill": "Ruby", "category": "language", "patterns": [r"\bruby\b"]},
    {"skill": "Scala", "category": "language", "patterns": [r"\bscala\b"]},
    {"skill": "Kotlin", "category": "language", "patterns": [r"\bkotlin\b"]},
    {"skill": "Swift", "category": "language", "patterns": [r"\bswift\b"]},
    {"skill": "R", "category": "language",
     # Found by hand-checking real postings, in order of how each was caught:
     # (1) an earlier version used an ASCII-only character class ([A-Za-z0-9])
     #     instead of \b, which doesn't recognize accented letters as word
     #     characters -- it let "R" in German words like "Rückfragen"
     #     ("questions") through as a false "standalone R" match, since it saw
     #     "R" followed by "ü" as a boundary. Python's \b IS Unicode-aware by
     #     default, so it correctly does NOT split "Rückfragen" at all.
     # (2) "R&D" (business term), "R+V" (a German insurance company name),
     #     "SAP R/3" (an ERP product name), "i.d.R." (common German
     #     abbreviation for "usually"), and "B&R" (an automation company)
     #     all still false-matched bare \bR\b -- excluded explicitly once
     #     found. Almost certainly not an exhaustive list of collisions;
     #     documented as a known limitation.
     "case_sensitive_patterns": [r"(?<!d\.)(?<!&)(?<!& )\bR\b(?!\s*[&+])(?!/\d)"]},
    {"skill": "C", "category": "language",
     # Same Unicode-boundary fix as R, plus business-term/company-name
     # collisions found by hand-checking real postings: "C-Level"/"C-suite",
     # "C-1" (numbered exec level), "C&A" (a retailer), "P&C Mentor" (an HR
     # role), and "C-arms" (a medical imaging device, not the language).
     "case_sensitive_patterns": [
         r"(?<!&)(?<!& )\bC\b(?!\+\+)(?!#)(?!-?\s?(?i:level|suite)\b)(?!-\d)(?!\s*&)(?!-arms?\b)(?<!\()"
     ]},

    # cloud
    {"skill": "AWS", "category": "cloud", "patterns": [r"\baws\b", r"\bamazon web services\b"]},
    {"skill": "Azure", "category": "cloud", "patterns": [r"\bazure\b"]},
    {"skill": "GCP", "category": "cloud", "patterns": [r"\bgcp\b", r"\bgoogle cloud\b"]},

    # data tools
    {"skill": "Spark", "category": "data_tool", "patterns": [r"\bspark\b", r"\bpyspark\b"]},
    {"skill": "Hadoop", "category": "data_tool", "patterns": [r"\bhadoop\b"]},
    {"skill": "Kafka", "category": "data_tool", "patterns": [r"\bkafka\b"]},
    {"skill": "Airflow", "category": "data_tool", "patterns": [r"\bairflow\b"]},
    {"skill": "dbt", "category": "data_tool", "patterns": [r"\bdbt\b"]},
    {"skill": "Snowflake", "category": "data_tool", "patterns": [r"\bsnowflake\b"]},
    {"skill": "Databricks", "category": "data_tool", "patterns": [r"\bdatabricks\b"]},
    {"skill": "Redshift", "category": "data_tool", "patterns": [r"\bredshift\b"]},
    {"skill": "BigQuery", "category": "data_tool", "patterns": [r"\bbigquery\b"]},

    # databases
    {"skill": "PostgreSQL", "category": "database", "patterns": [r"\bpostgres(ql)?\b"]},
    {"skill": "MySQL", "category": "database", "patterns": [r"\bmysql\b"]},
    {"skill": "MongoDB", "category": "database", "patterns": [r"\bmongodb\b", r"\bmongo\b"]},
    {"skill": "Cassandra", "category": "database", "patterns": [r"\bcassandra\b"]},
    {"skill": "Redis", "category": "database", "patterns": [r"\bredis\b"]},

    # BI tools
    {"skill": "Tableau", "category": "bi_tool", "patterns": [r"\btableau\b"]},
    {"skill": "Power BI", "category": "bi_tool", "patterns": [r"\bpower\s?bi\b"]},
    {"skill": "Looker", "category": "bi_tool", "patterns": [r"\blooker\b"]},
    {"skill": "Excel", "category": "bi_tool", "patterns": [r"\bexcel\b"]},

    # frameworks
    {"skill": "React", "category": "framework", "patterns": [r"\breact(\.js)?\b(?!\s?native)"]},
    {"skill": "React Native", "category": "framework", "patterns": [r"\breact native\b"]},
    {"skill": "Angular", "category": "framework", "patterns": [r"\bangular\b"]},
    {"skill": "Vue", "category": "framework", "patterns": [r"\bvue(\.js)?\b"]},
    {"skill": "Django", "category": "framework", "patterns": [r"\bdjango\b"]},
    {"skill": "Flask", "category": "framework", "patterns": [r"\bflask\b"]},
    {"skill": "Node.js", "category": "framework", "patterns": [r"\bnode(\.js)?\b"]},
    # "Spring"/"Express" bare would be terrible keywords (season/verb, "express
    # delivery") -- only match the unambiguous compound forms, same lesson as Go.
    {"skill": "Spring", "category": "framework",
     "patterns": [r"\bspring\s?boot\b", r"\bspring framework\b"]},
    {"skill": "Express", "category": "framework", "patterns": [r"\bexpress\.?js\b"]},

    # web tech (explicitly requested as a frontend domain signal)
    {"skill": "HTML", "category": "web_tech", "patterns": [r"\bhtml5?\b"]},
    {"skill": "CSS", "category": "web_tech", "patterns": [r"\bcss3?\b"]},

    # devops
    {"skill": "Docker", "category": "devops", "patterns": [r"\bdocker\b"]},
    {"skill": "Kubernetes", "category": "devops", "patterns": [r"\bkubernetes\b", r"\bk8s\b"]},
    {"skill": "Terraform", "category": "devops", "patterns": [r"\bterraform\b"]},
    {"skill": "Git", "category": "devops", "patterns": [r"\bgit\b(?!hub|lab)"]},

    # ML
    {"skill": "Machine Learning", "category": "ml", "patterns": [r"\bmachine learning\b"]},
    {"skill": "TensorFlow", "category": "ml", "patterns": [r"\btensorflow\b"]},
    {"skill": "PyTorch", "category": "ml", "patterns": [r"\bpytorch\b"]},

    # Non-CS tools -- a deliberately modest, bounded list (not exhaustive,
    # same as the CS entries above) covering the tools that showed up
    # repeatedly while hand-checking real other/uncategorized postings for
    # the broad-field pass below: Salesforce/HubSpot/Mailchimp for sales &
    # marketing roles, QuickBooks for finance/accounting, SAP for the many
    # German finance/ERP postings, Photoshop/Illustrator/Canva for
    # creative/marketing roles, AutoCAD for skilled-trades/technical-drafting
    # roles, Zendesk for support roles. Reuses the exact same
    # extract_skills()/posting_skills fact-table machinery as the CS skills
    # above -- one row per (source, job_id, skill) either way, distinguished
    # by category, not a parallel system.
    {"skill": "Salesforce", "category": "sales_tool", "patterns": [r"\bsalesforce\b"]},
    {"skill": "HubSpot", "category": "sales_tool", "patterns": [r"\bhubspot\b"]},
    {"skill": "Mailchimp", "category": "sales_tool", "patterns": [r"\bmailchimp\b"]},
    {"skill": "QuickBooks", "category": "finance_tool", "patterns": [r"\bquickbooks\b"]},
    {"skill": "SAP", "category": "erp_tool", "patterns": [r"\bsap\b"]},
    {"skill": "Photoshop", "category": "design_tool", "patterns": [r"\bphotoshop\b"]},
    {"skill": "Illustrator", "category": "design_tool", "patterns": [r"\billustrator\b"]},
    {"skill": "Canva", "category": "design_tool", "patterns": [r"\bcanva\b"]},
    {"skill": "AutoCAD", "category": "design_tool", "patterns": [r"\bautocad\b"]},
    {"skill": "Zendesk", "category": "support_tool", "patterns": [r"\bzendesk\b"]},
]

_compiled_skills = [
    (skill, patterns) for skill, patterns in (
        (
            entry["skill"],
            [re.compile(p, re.IGNORECASE) for p in entry.get("patterns", [])]
            + [re.compile(p) for p in entry.get("case_sensitive_patterns", [])],
        )
        for entry in SKILL_DICTIONARY
    )
    if skill != "Go"
]

# "Go" special case: "Golang" is unambiguous and always counts. Bare "Go" is
# the single riskiest keyword in this whole dictionary -- an ordinary English
# word capitalized constantly (sentence starts, "Let's Go!", "Go with the
# flow"), on top of business phrases "Go-Live"/"Go to Market". Hand-checking
# real postings found a recruiting-agency template ("das „Go" geben") reused
# across ~55 near-duplicate non-tech (tax advisor) postings, all inflating the
# count -- while every GENUINE mention found co-occurred with another
# recognized language in the same posting ("Python, Go", "Rust oder Go", "Go
# and TypeScript"). So bare "Go" only counts if that co-occurrence holds.
_golang_pattern = re.compile(r"\bgolang\b", re.IGNORECASE)
_bare_go_pattern = re.compile(r"\bGo\b(?!-?\s?(?i:live)\b)(?!\s+(?i:to\s+market))(?!-(?i:to-market))")
_GO_LANGUAGE_CONTEXT = {"Python", "Java", "Rust", "C++", "C#", "TypeScript",
                         "JavaScript", "Kotlin", "Swift", "PHP", "Ruby", "Scala", "C"}


def extract_skills(text):
    if not text:
        return []
    found = []
    for skill, patterns in _compiled_skills:
        if any(p.search(text) for p in patterns):
            found.append(skill)
    if _golang_pattern.search(text):
        found.append("Go")
    elif _bare_go_pattern.search(text) and (_GO_LANGUAGE_CONTEXT & set(found)):
        found.append("Go")
    return found


extract_skills_udf = F.udf(extract_skills, ArrayType(StringType()))

# Strips soft hyphens (U+00AD) and zero-width characters before any pattern
# matching runs. Found by hand-checking a real title that should have matched
# "Softwareentwickler" but didn't: "Software­entwickler" (a soft hyphen
# embedded between "Software" and "entwickler", almost certainly carried over
# from a web page's line-break hint) is invisible when printed but isn't a
# word character `\b`/`[\s-]?` will bridge, so it silently splits the token in
# two for matching purposes even though it reads as one word. Applied to
# BOTH the skill-matching text and (inside classify_domain, on the raw title)
# domain-classification text, not just this one word, since any title/
# description scraped from the web could carry the same invisible characters.
_INVISIBLE_CHARS_RE = re.compile(r"[­​‌‍]")

matching_text = F.regexp_replace(
    F.concat_ws(" ", F.col("title"), F.col("description_clean"), F.array_join(F.col("tags"), " ")),
    r"[­​‌‍]",
    "",
)

# Computed once here, used twice: as a domain-classification signal below, and
# exploded into the posting_skills fact table further down.
deduped = deduped.withColumn("skills", extract_skills_udf(matching_text))

# COMMAND ----------

# MAGIC %md ### Domain classification: title first, then skills, then tags
# MAGIC
# MAGIC Rule-based (regex + skill-signal) fixed taxonomy, not ML. Priority order:
# MAGIC 1. Title -- the strongest, most intentional signal.
# MAGIC 2. Extracted skills -- e.g. dbt/Airflow/Spark/Snowflake mentioned anywhere
# MAGIC    in a posting is a real data-engineering signal even if the title itself
# MAGIC    (e.g. plain "Software Engineer") says nothing. Same logic for other
# MAGIC    domains: React/Vue/CSS/HTML -> frontend, Django/Flask/Spring/Express ->
# MAGIC    backend, React Native/Swift/Kotlin -> mobile, Tableau/Power BI/Looker ->
# MAGIC    data analyst. Deliberately excludes generic/ubiquitous skills (SQL,
# MAGIC    Excel, Node.js) from these signal sets -- they're used across so many
# MAGIC    roles that they'd add noise, not signal. `full-stack` has no skill
# MAGIC    signal at all: skill co-occurrence (e.g. a data engineer's posting
# MAGIC    mentioning both React or Python and Docker) is much weaker evidence of
# MAGIC    an actual full-stack ROLE than the other mappings are of theirs, so it
# MAGIC    stays title-only rather than guessing.
# MAGIC 3. Tags -- weakest, noisiest signal (RemoteOK/Arbeitnow tags are often
# MAGIC    generic or irrelevant, see BUILD_LOG.md).
# MAGIC
# MAGIC Titles/skills that don't clearly match stay in `other/uncategorized` --
# MAGIC forcing a weak match would be worse than an honest "doesn't fit" bucket.
# MAGIC `DOMAIN_RULES` includes German-language title patterns alongside the
# MAGIC English ones (Softwareentwickler/Vollstack-Entwickler -> full-stack,
# MAGIC Datenanalyst(in) -> data analyst, Dateningenieur -> data engineer,
# MAGIC Frontend-/Backend-/App-/iOS-/Android-Entwickler(in) -> their English
# MAGIC counterparts) so Arbeitnow's mostly-German postings aren't systematically
# MAGIC under-represented just because the rules were English-only. Verified by
# MAGIC hand-checking real "other"-bucket titles first: most of that bucket is
# MAGIC genuinely non-tech (Buchhalter/accountant, Personaldisponent/HR-staffing,
# MAGIC Kundenberater/customer-service roles are the bulk of it, in German and
# MAGIC English alike) -- so a large residual "other" bucket after this pass is
# MAGIC expected, not a sign the classifier is still missing something.

# COMMAND ----------

# German titles get their own patterns per domain rather than a translation
# step -- same taxonomy, same priority order (title -> skills -> tags), just
# covering the German compound-noun equivalents of each English title pattern
# above. `GENDER_SUFFIX` covers the several ways German job postings mark
# gender-neutral/feminine forms on a role noun: "Entwickler", "Entwicklerin",
# "Entwickler:in", "Entwickler*in", "Entwickler/in" all need to match.
# Deliberately NOT adding bare "Entwickler"/"Programmierer" (German for
# "developer"/"programmer" with no stack/domain qualifier) -- same reasoning
# as bare English "Developer" staying unclassified: too vague to guess
# frontend/backend/full-stack from. "Softwareentwickler" is the one
# qualified-but-still-generic exception mapped straight to full-stack, since
# unlike English postings (which usually name a stack), German dev postings
# very commonly use unqualified "(Software-)Entwickler" as the generic title
# for what would be called a full-stack/generalist developer role in English.
GENDER_SUFFIX = r"(?:[:/*]?in)?"

DOMAIN_RULES = [
    ("data engineer", [
        r"\bdata engineer", r"\bdata engineering\b", r"\betl\b", r"\betl developer",
        r"\betl architect", r"\betl engineer", r"\bdata platform\b", r"\banalytics engineer\b",
        r"\bdata infrastructure\b", r"\bdata infra\b", r"\bdata pipeline\b",
        r"\bbig data engineer\b", r"\bbig data developer\b",
        r"\bmachine learning engineer\b", r"\bml engineer\b",
        r"\bdataops\b", r"\bdata ops\b", r"software engineer,?\s*data\b",
        # German
        rf"\bdaten[\s-]?ingenieur{GENDER_SUFFIX}\b", rf"\betl[\s-]?entwickler{GENDER_SUFFIX}\b",
    ]),
    ("data analyst", [
        r"\bdata analyst", r"\bbusiness analyst", r"\bbi analyst\b", r"\bbusiness intelligence\b",
        r"\breporting analyst\b", r"\bdata analytics\b", r"\bbi developer\b",
        r"\binsights analyst\b", r"\banalytics manager\b", r"\bdata scientist\b", r"\bdata science\b",
        # German
        rf"\bdaten[\s-]?analyst{GENDER_SUFFIX}\b", rf"\bdaten[\s-]?analytiker{GENDER_SUFFIX}\b",
    ]),
    ("mobile", [
        r"\bios developer", r"\bandroid developer", r"\bmobile developer", r"\bmobile engineer",
        r"\breact native\b", r"\bflutter\b", r"\bkotlin developer\b", r"\bswift developer\b",
        # German
        rf"\bapp[\s-]?entwickler{GENDER_SUFFIX}\b", rf"\bios[\s-]?entwickler{GENDER_SUFFIX}\b",
        rf"\bandroid[\s-]?entwickler{GENDER_SUFFIX}\b",
    ]),
    ("frontend", [
        r"\bfront[\s-]?end", r"\bui developer", r"\bui engineer", r"\bui/ux engineer",
        r"\breact developer", r"\bangular developer", r"\bvue developer", r"\bweb developer",
        # German (front[\s-]?end above already matches "Frontend-Entwickler" as
        # a substring, but spelled out explicitly so this rule's German
        # coverage doesn't silently depend on that overlap)
        rf"\bfrontend[\s-]?entwickler{GENDER_SUFFIX}\b", rf"\bweb[\s-]?entwickler{GENDER_SUFFIX}\b",
    ]),
    ("backend", [
        r"\bback[\s-]?end", r"\bapi developer", r"\bapi engineer", r"\bserver-side\b",
        r"\bplatform engineer\b",
        # German
        rf"\bbackend[\s-]?entwickler{GENDER_SUFFIX}\b",
    ]),
    ("full-stack", [
        r"\bfull[\s-]?stack",
        # German -- see the note above GENDER_SUFFIX for why bare
        # "Softwareentwickler" (not just "Vollstack-Entwickler") lands here.
        # Also covers "<Language> Entwickler" titles ("Python Entwickler",
        # "Java Entwickler") for the same reason -- a language-qualified but
        # otherwise generic developer title, found by hand-checking real
        # postings still in other/uncategorized ("Senior Python Entwickler",
        # "Beratender Senior Java Entwickler"). Deliberately excludes SAP --
        # "SAP Entwickler" is an ERP-specific title, same reasoning as why
        # "(Senior) Developer SAP ABAP" stays other/uncategorized in English.
        rf"\bvoll[\s-]?stack{GENDER_SUFFIX}\b", rf"\bvollstack[\s-]?entwickler{GENDER_SUFFIX}\b",
        rf"\bsoftware[\s-]?entwickler{GENDER_SUFFIX}\b",
        rf"\b(?:python|java|javascript|typescript|php|ruby|kotlin|swift|scala|rust|c\+\+|c#)"
        rf"[\s-]?entwickler{GENDER_SUFFIX}\b",
    ]),
]
_domain_compiled = [(name, [re.compile(p, re.IGNORECASE) for p in pats]) for name, pats in DOMAIN_RULES]

# Skill co-occurrence signals, checked in the same priority order as titles
# above (minus full-stack, which has none -- see the markdown cell above for why).
# Deliberately excludes generic/ubiquitous skills (SQL, Excel, Node.js, Git,
# Docker) that show up across nearly every domain and would add noise.
#
# ML/TensorFlow/PyTorch route to "data engineer" -- an imperfect compromise,
# but the fixed taxonomy has no ML/AI-specific bucket, and data engineering is
# the closest adjacent discipline (both are data/model-centric engineering
# work). Found while checking real "other"-bucket postings by hand: several
# genuine AI/ML/Computer-Vision engineer roles had no other home.
DOMAIN_SKILL_SIGNALS = [
    ("data engineer", {"dbt", "Airflow", "Spark", "Snowflake", "Databricks", "Redshift", "BigQuery",
                        "Hadoop", "Kafka", "Machine Learning", "TensorFlow", "PyTorch"}),
    ("data analyst", {"Tableau", "Power BI", "Looker"}),
    ("mobile", {"React Native", "Swift", "Kotlin"}),
    ("frontend", {"React", "Vue", "Angular", "CSS", "HTML"}),
    ("backend", {"Django", "Flask", "Spring", "Express", "PHP"}),
]


def classify_domain(title, tags_list, skills_list):
    title_text = _INVISIBLE_CHARS_RE.sub("", title) if title else ""
    tags_text = _INVISIBLE_CHARS_RE.sub("", " ".join(tags_list)) if tags_list else ""
    skills_set = set(skills_list) if skills_list else set()

    for name, patterns in _domain_compiled:
        if any(p.search(title_text) for p in patterns):
            return name

    for name, signal_skills in DOMAIN_SKILL_SIGNALS:
        if signal_skills & skills_set:
            return name

    for name, patterns in _domain_compiled:
        if any(p.search(tags_text) for p in patterns):
            return name

    return "other/uncategorized"


classify_domain_udf = F.udf(classify_domain, StringType())

cleaned_postings = deduped.withColumn(
    "domain", classify_domain_udf(F.col("title"), F.col("tags"), F.col("skills"))
)

# COMMAND ----------

# MAGIC %md ### Broad-field classification: a second, coarser dimension
# MAGIC
# MAGIC `domain` above is the primary CS-domain taxonomy and stays the main
# MAGIC dashboard story. This is a separate, secondary dimension applied to give
# MAGIC the large `other/uncategorized` CS-domain bucket some structure, since
# MAGIC hand-checking real postings confirmed most of it is genuinely non-tech
# MAGIC work (accounting, sales/marketing, healthcare, skilled trades,
# MAGIC administrative/support, education, retail, hospitality, manufacturing/
# MAGIC logistics, legal/HR), not a CS-domain classifier gap -- see BUILD_LOG.md.
# MAGIC Deliberately simpler than the CS-domain classifier (title -> a modest
# MAGIC tool-based skill signal -> tags, same priority order, but shallower
# MAGIC keyword lists per bucket): this only needs to be directionally useful,
# MAGIC not as rigorously verified as the primary taxonomy -- confirmed with the
# MAGIC project owner as the final round of broad_field buckets, not chasing the
# MAGIC unclassified percentage further after this. Every posting gets a
# MAGIC broad_field value, including ones with a real CS domain (those just
# MAGIC won't match any non-tech bucket and will fall to "Other" here, which is
# MAGIC fine -- this column is only surfaced in the dashboard's separate "Beyond
# MAGIC Tech" section, not mixed into the primary domain breakdown).

# COMMAND ----------

BROAD_FIELD_RULES = [
    # Checked in this order so specific service/trade titles resolve before
    # broader "customer-facing"/"medical-adjacent" keywords could misfire
    # (e.g. "Medical Equipment Service Technician" is Skilled Trades, not
    # Healthcare -- it matches the technician pattern first).
    ("Skilled Trades", [
        r"\belektri(k|ker)\b", r"\belektroniker\b", r"\bmechatroniker\b",
        r"\btechniker\b", r"\bservicetechniker\b", r"\bservice technician\b",
        r"\bmonteur\b", r"\bhandwerk", r"\bbauleiter\b", r"\bbauingenieur\b",
        r"\belectrician\b", r"\bplumber\b", r"\bhvac\b", r"\bfahrer\b",
        r"\bschädlingsbekämpfer\b", r"\bkfz\b",
    ]),
    ("Healthcare", [
        r"\bpflegefachkraft\b", r"\bpflegekraft\b", r"\baltenpfleger\b",
        r"\bkrankenpfleger\b", r"\bkrankenschwester\b", r"\bphysiotherapeut\b",
        r"\bmedizinische[nr]? fachangestellte\b", r"\bnurse\b", r"\bphysician\b",
        r"\bhealthcare\b",
    ]),
    ("Finance & Accounting", [
        r"\bbuchhalt", r"\bbilanzbuchhalter\b", r"\bsteuerfach", r"\bsteuerberat",
        r"\baccountant\b", r"\bcontroller\b", r"\bcontrolling\b",
        r"\bfinanzbuchhalt", r"\bkreditsachbearbeit", r"\bkreditorenbuchhalter\b",
        r"\bdebitorenbuchhalter\b", r"\baccounting\b", r"\bpayroll\b",
        r"\blohnbuchhalt", r"\btax\b", r"\bauditor\b", r"\btreasury\b", r"\bkyc\b",
    ]),
    ("Sales & Marketing", [
        r"\bsales\b", r"\bvertrieb", r"\bmarketing\b", r"\baccount executive\b",
        r"\baccount manager\b", r"\bkey account\b", r"\bbusiness development\b",
        r"\bsdr\b", r"\bcustomer success\b", r"\binfluencer\b", r"\bsocial media\b",
        r"\bseo\b", r"\bsea\b", r"\bcontent creator\b", r"\bcopywriter\b",
        r"\bgrowth\b", r"\bpaid (media|social|acquisition)\b",
        r"\bperformance marketing\b", r"\bkundenberater", r"\bkundenbetreuung",
        r"\bgraphic designer\b", r"\bgrafikdesigner",
    ]),
    ("Administrative/Support", [
        r"\bassistenz\b", r"\boffice manager\b", r"\bverwaltung\b",
        r"\bsachbearbeiter\b", r"\badministrative assistant\b", r"\breceptionist\b",
        r"\bpersonalreferent\b", r"\bpersonaldisponent\b", r"\bhuman resources\b", r"\bhr\b",
        r"\bteam assistant\b", r"\boffice coordinator\b", r"\bcustomer service\b",
        r"\bkundenservice\b", r"\bkundendienst\b",
    ]),
    ("Education", [
        r"\blehrer\b", r"\bdozent\b", r"\bausbilder\b", r"\bteacher\b",
        r"\bprofessor\b", r"\btutor\b", r"\binstructor\b",
    ]),
    # Fourth and final round of broad_field buckets -- same lightweight,
    # directionally-useful keyword approach as the buckets above, not the
    # hand-verification rigor used for the CS-domain taxonomy. This is the
    # last round; a long tail of niche fields will still fall to "Other".
    ("Retail & Customer Service", [
        r"\bstore manager\b", r"\bfilialleiter\b", r"\bretail\b", r"\bcashier\b",
        r"\bverkäufer", r"\bverkaufsberater", r"\bfiliale\b", r"\bshop\b",
    ]),
    ("Hospitality & Food Service", [
        r"\bhotel\b", r"\brestaurant\b", r"\bkoch\b", r"\bküche\b",
        r"\bgastronomie\b", r"\bbarista\b", r"\bkellner\b", r"\bhospitality\b",
    ]),
    ("Manufacturing & Logistics", [
        r"\bproduktion", r"\bfertigung\b", r"\blager\b", r"\blogistik\b",
        r"\blogistics\b", r"\bwarehouse\b", r"\bsupply chain\b", r"\beinkauf",
        r"\bprocurement\b", r"\bmanufacturing\b", r"\bmontage\b",
    ]),
    ("Legal & HR", [
        r"\brecruiter\b", r"\brecruiting\b", r"\bpersonalberater\b",
        r"\bpersonalsachbearbeiter\b", r"\brechtsanwalt\b", r"\bjurist\b",
        r"\blegal\b", r"\bcompliance\b", r"\blawyer\b", r"\battorney\b",
        r"\bra-spezialist", r"\bkanzlei\b",
    ]),
]
_broad_field_compiled = [(name, [re.compile(p, re.IGNORECASE) for p in pats]) for name, pats in BROAD_FIELD_RULES]

# Non-CS tool signal, same idea as DOMAIN_SKILL_SIGNALS but for the broad-field
# pass -- a posting mentioning Salesforce/HubSpot is a real sales/marketing
# signal even if the title alone doesn't say so.
BROAD_FIELD_SKILL_SIGNALS = [
    ("Sales & Marketing", {"Salesforce", "HubSpot", "Mailchimp", "Photoshop", "Illustrator", "Canva"}),
    ("Finance & Accounting", {"QuickBooks"}),
    ("Skilled Trades", {"AutoCAD"}),
    ("Administrative/Support", {"Zendesk"}),
]


def classify_broad_field(title, tags_list, skills_list):
    title_text = _INVISIBLE_CHARS_RE.sub("", title) if title else ""
    tags_text = _INVISIBLE_CHARS_RE.sub("", " ".join(tags_list)) if tags_list else ""
    skills_set = set(skills_list) if skills_list else set()

    for name, patterns in _broad_field_compiled:
        if any(p.search(title_text) for p in patterns):
            return name

    for name, signal_skills in BROAD_FIELD_SKILL_SIGNALS:
        if signal_skills & skills_set:
            return name

    for name, patterns in _broad_field_compiled:
        if any(p.search(tags_text) for p in patterns):
            return name

    return "Other"


classify_broad_field_udf = F.udf(classify_broad_field, StringType())

cleaned_postings = cleaned_postings.withColumn(
    "broad_field", classify_broad_field_udf(F.col("title"), F.col("tags"), F.col("skills"))
)

# COMMAND ----------

# MAGIC %md ### Write cleaned_postings and the posting_skills fact table
# MAGIC
# MAGIC `skills` was only ever an intermediate column (input to both domain and
# MAGIC broad_field classification) -- dropped before the final write since
# MAGIC `posting_skills` is the properly normalized place for that information,
# MAGIC not a denormalized array sitting on the postings table too.

# COMMAND ----------

posting_skills = (
    cleaned_postings
    .withColumn("skill", F.explode(F.col("skills")))
    .select("source", "job_id", "skill")
)

cleaned_postings = cleaned_postings.drop("skills").withColumn("_silver_processed_at", F.current_timestamp())

cleaned_postings.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.silver.cleaned_postings")
print(f"{CATALOG}.silver.cleaned_postings: {cleaned_postings.count()} rows")

posting_skills.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.silver.posting_skills")
print(f"{CATALOG}.silver.posting_skills: {posting_skills.count()} rows")

# COMMAND ----------

# MAGIC %md ### Skill dictionary as a queryable reference table
# MAGIC
# MAGIC Documents the maintained keyword list itself (skill -> category) so Gold/
# MAGIC dashboard can group by category, and so the dictionary's current coverage
# MAGIC is inspectable data, not just buried in notebook code.

# COMMAND ----------

skill_dictionary_df = spark.createDataFrame(
    [(e["skill"], e["category"]) for e in SKILL_DICTIONARY],
    schema=["skill", "category"],
)
skill_dictionary_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.silver.skill_dictionary")

print(f"{CATALOG}.silver.skill_dictionary: {skill_dictionary_df.count()} rows")

# COMMAND ----------

# MAGIC %md ### Domain and broad-field distribution (sanity check on every run)

# COMMAND ----------

display(spark.sql(f"""
    SELECT domain, count(*) AS n
    FROM {CATALOG}.silver.cleaned_postings
    GROUP BY domain ORDER BY n DESC
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT broad_field, count(*) AS n
    FROM {CATALOG}.silver.cleaned_postings
    GROUP BY broad_field ORDER BY n DESC
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT broad_field, count(*) AS n
    FROM {CATALOG}.silver.cleaned_postings
    WHERE domain = 'other/uncategorized'
    GROUP BY broad_field ORDER BY n DESC
"""))

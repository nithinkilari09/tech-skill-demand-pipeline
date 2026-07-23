# Databricks notebook source
# Gold: skill-demand-by-domain aggregates, served straight from Databricks'
# own SQL warehouse for the dashboard (no separate data warehouse -- see
# README.md for why). Four tables, all small (low thousands of rows at most
# at this data scale) and cheap to fully recompute from Silver every run,
# same "full recompute, not incremental" reasoning as Silver's dedup step:
# simpler and fully correct every time at this scale.
#
# - domain_summary            : posting count per CS-domain bucket
# - skill_demand_by_domain    : skill x CS-domain mention counts (the
#                               primary dashboard story: which tools show up
#                               most for data engineers vs. frontend vs. ...)
# - broad_field_summary       : posting count per broad_field bucket
# - skill_demand_by_broad_field: skill x broad_field mention counts (the
#                               "Beyond Tech" dashboard section)
#
# Both skill-demand tables include every skill in skill_dictionary (CS and
# non-CS) against both dimensions -- e.g. Excel shows up under both `data
# analyst` (domain) and `Finance & Accounting` (broad_field), which is a
# real, correct signal, not a modeling mistake to avoid.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "tech_skill_demand"

# COMMAND ----------

cleaned_postings = spark.table(f"{CATALOG}.silver.cleaned_postings")
posting_skills = spark.table(f"{CATALOG}.silver.posting_skills")
skill_dictionary = spark.table(f"{CATALOG}.silver.skill_dictionary")

# Only join in the columns Gold actually needs from cleaned_postings --
# posting_skills is already the properly normalized (source, job_id, skill)
# fact table, this just attaches the two classification dimensions to each
# skill mention.
postings_dim = cleaned_postings.select("source", "job_id", "domain", "broad_field")

skills_with_dims = (
    posting_skills
    .join(postings_dim, on=["source", "job_id"], how="inner")
    .join(skill_dictionary, on="skill", how="left")
)

# COMMAND ----------

# MAGIC %md ### domain_summary and skill_demand_by_domain

# COMMAND ----------

domain_summary = (
    cleaned_postings.groupBy("domain")
    .agg(F.count("*").alias("posting_count"))
    .orderBy(F.col("posting_count").desc())
)

domain_summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.domain_summary")
print(f"{CATALOG}.gold.domain_summary: {domain_summary.count()} rows")

# COMMAND ----------

skill_demand_by_domain = (
    skills_with_dims.groupBy("domain", "skill", "category")
    .agg(F.count("*").alias("mention_count"))
    .orderBy(F.col("domain"), F.col("mention_count").desc())
)

skill_demand_by_domain.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.skill_demand_by_domain")
print(f"{CATALOG}.gold.skill_demand_by_domain: {skill_demand_by_domain.count()} rows")

# COMMAND ----------

# MAGIC %md ### broad_field_summary and skill_demand_by_broad_field

# COMMAND ----------

broad_field_summary = (
    cleaned_postings.groupBy("broad_field")
    .agg(F.count("*").alias("posting_count"))
    .orderBy(F.col("posting_count").desc())
)

broad_field_summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.broad_field_summary")
print(f"{CATALOG}.gold.broad_field_summary: {broad_field_summary.count()} rows")

# COMMAND ----------

skill_demand_by_broad_field = (
    skills_with_dims.groupBy("broad_field", "skill", "category")
    .agg(F.count("*").alias("mention_count"))
    .orderBy(F.col("broad_field"), F.col("mention_count").desc())
)

skill_demand_by_broad_field.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.skill_demand_by_broad_field")
print(f"{CATALOG}.gold.skill_demand_by_broad_field: {skill_demand_by_broad_field.count()} rows")

# COMMAND ----------

# MAGIC %md ### Sanity check: top 5 skills per CS domain (excluding other/uncategorized)

# COMMAND ----------

display(spark.sql(f"""
    SELECT domain, skill, mention_count FROM (
        SELECT domain, skill, mention_count,
               row_number() OVER (PARTITION BY domain ORDER BY mention_count DESC) AS rn
        FROM {CATALOG}.gold.skill_demand_by_domain
        WHERE domain != 'other/uncategorized'
    ) WHERE rn <= 5
    ORDER BY domain, mention_count DESC
"""))

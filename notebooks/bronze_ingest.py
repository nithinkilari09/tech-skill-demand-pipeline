# Databricks notebook source
# Bronze: raw job postings, landed as-is, into Delta Lake.
#
# One notebook, parameterized by `source` (remoteok | arbeitnow) rather than one
# notebook per source -- the two APIs return incompatible schemas (RemoteOK has
# `position`/`company`, Arbeitnow has `title`/`company_name`, etc.), so each gets
# its own Bronze table rather than a forced merge. Merging/normalizing is Silver's
# job, not Bronze's -- Bronze stays "raw as landed."
#
# Uses Auto Loader (cloudFiles) with Trigger.AvailableNow: processes whatever new
# files have landed since the last run, then stops -- not a 24/7 stream. This is
# the same trigger Databricks Workflows will schedule daily once orchestration is
# wired up; running it manually here is the same code path, just not yet on a
# schedule.

# COMMAND ----------

dbutils.widgets.text("source", "remoteok")
source = dbutils.widgets.get("source")
assert source in ("remoteok", "arbeitnow"), f"unknown source: {source}"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

BUCKET = "tech-skill-demand-pipeline-raw-039624954996"
CATALOG = "tech_skill_demand"

raw_path = f"s3://{BUCKET}/source={source}/"
table_name = f"{CATALOG}.bronze.{source}_postings"
checkpoint_path = f"s3://{BUCKET}/warehouse/_checkpoints/bronze_{source}/"
schema_location = f"s3://{BUCKET}/warehouse/_schemas/bronze_{source}/"

# COMMAND ----------

raw_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(raw_path)
    .withColumn("_bronze_ingested_at", current_timestamp())
    # input_file_name() isn't supported under Unity Catalog on serverless (Spark
    # Connect) compute -- _metadata.file_path is the supported equivalent.
    .withColumn("_source_file", col("_metadata.file_path"))
)

query = (
    raw_stream.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(table_name)
)
query.awaitTermination()

# COMMAND ----------

result = spark.sql(f"SELECT count(*) AS row_count FROM {table_name}").collect()[0]
print(f"{table_name}: {result['row_count']} rows")

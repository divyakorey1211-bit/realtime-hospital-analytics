from pyspark.sql.types import *
from pyspark.sql.functions import *

# -------------------------
# ADLS Configuration
# -------------------------
spark.conf.set(
  "fs.azure.account.key.<<Storageaccount_name>>.dfs.core.windows.net",
  "<<Storage_Account_access_key>>"
)

bronze_path = "abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/<<path>>"
silver_path = "abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/<<path>>"


# -------------------------
# Read from Bronze
# -------------------------
bronze_df = (
    spark.readStream
    .format("delta")
    .load(bronze_path)
)

# -------------------------
# Define Schema (MATCH SOURCE)
# -------------------------
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", StringType(), True),

    StructField("patient_id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("department", StringType(), True),

    StructField("admission_time", StringType(), True),
    StructField("discharge_time", StringType(), True),

    StructField("bed_id", IntegerType(), True),
    StructField("hospital_id", IntegerType(), True),
    StructField("wait_time_minutes", IntegerType(), True)
])

# -------------------------
# Parse JSON
# -------------------------
parsed_df = (
    bronze_df
    .withColumn("data", from_json(col("raw_json"), schema))
    .select("data.*")
)

# -------------------------
# Type Casting
# -------------------------
clean_df = (
    parsed_df
    .withColumn("event_time", to_timestamp("event_time"))
    .withColumn("admission_time", to_timestamp("admission_time"))
    .withColumn("discharge_time", to_timestamp("discharge_time"))
)

# -------------------------
# Data Quality Rules
# -------------------------
clean_df = clean_df.withColumn(
    "admission_time",
    when(
        col("admission_time").isNull() | (col("admission_time") > current_timestamp()),
        current_timestamp()
    ).otherwise(col("admission_time"))
)

clean_df = clean_df.withColumn(
    "age",
    when(col("age") > 100, floor(rand() * 90 + 1).cast("int"))
    .otherwise(col("age"))
)

# -------------------------
# Schema Evolution Guard
# -------------------------
expected_cols = [
    "event_id", "event_time", "patient_id", "gender", "age",
    "department", "admission_time", "discharge_time",
    "bed_id", "hospital_id", "wait_time_minutes"
]

for c in expected_cols:
    if c not in clean_df.columns:
        clean_df = clean_df.withColumn(c, lit(None))

# -------------------------
# Write to Silver
# -------------------------
(
    clean_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("mergeSchema", "true")
    .option("checkpointLocation", "dbfs:/tmp/checkpoints/silver_patient_event")
    .start(silver_path)
)



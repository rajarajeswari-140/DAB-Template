# Databricks notebook source

import dlt
from pyspark.sql import functions as F

DEMO_SOURCE_PATH = "/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"

# === Bronze table ===
BRONZE_TABLE_NAME = "v4c.bronze.sf-fire_calls"

# If you have a UC volume already, use it here; otherwise fallback to DBFS for demo:
# SCHEMA_LOCATION = "/Volumes/bronze/schema_location"
CHECKPOINT_LOCATION = "/Volumes/bronze/checkpoints_tripdata"

# COMMAND ----------

def clean_colname(col: str) -> str:
    """
    Make UC-safe column names:
    - lower-case
    - replace invalid chars ' ,;{}()\\n\\t=' with underscore
    - collapse multiple underscores
    - strip leading/trailing underscores
    """
    import re
    s = col.lower()
    s = re.sub(r"[ ,;{}()\n\t=]+", "_", s)   # replace invalid chars with underscores
    s = re.sub(r"__+", "_", s)               # collapse multiple underscores
    s = s.strip("_")
    # Optional: avoid leading digits by prefixing if needed
    if re.match(r"^\d", s):
        s = f"col_{s}"
    return s

@dlt.table(
    name="sf_fire_calls",
    comment="Bronze demo via batch ingestion "
)
def bronze_demo_batch():
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(DEMO_SOURCE_PATH)
        .withColumn("load_dt", F.current_timestamp())
        .withColumn("source_file", F.col("_metadata.file_path"))
    )
    
    renamed_cols = [F.col(c).alias(clean_colname(c)) for c in df.columns]
    df_clean = df.select(*renamed_cols)

    return df_clean


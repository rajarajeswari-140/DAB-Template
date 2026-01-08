# Databricks notebook source

# Databricks notebook source

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Helpers: parse timestamp with multiple possible formats
def parse_ts_multi(col):
    # Try AM/PM, then 24h, else None
    return F.coalesce(
        F.to_timestamp(F.col(col), "MM/dd/yyyy HH:mm:ss a"),
        F.to_timestamp(F.col(col), "MM/dd/yyyy HH:mm:ss"),
        F.to_timestamp(F.col(col), "MM/dd/yyyy HH:mm"),
        F.to_timestamp(F.col(col))  # last resort (Spark infers if ISO)
    )

@dlt.table(
    name="sf_fire_calls_silver",
    comment="Silver: SF Fire Calls — parsed timestamps, normalized text, deduped on call_number, derived KPIs."
)
@dlt.expect_all({
    "has_call_number": "call_number IS NOT NULL",
    "has_some_time": "available_ts IS NOT NULL OR call_date IS NOT NULL OR load_dt IS NOT NULL"
})
def silver_sf_fire_calls():
    df = dlt.read("v4c.bronze.sf_fire_calls")   # if Bronze is streaming, you can switch to dlt.read_stream("sf_fire_calls")

    # Parse timestamps
    df = (
        df
        .withColumn("available_ts", parse_ts_multi("available_dttm"))  # from string
        .withColumn("call_date", F.col("call_date").cast("date"))      # already date in Bronze per your schema
        # Canonical call datetime: prefer available_ts, else call_date at midnight, else ingestion time
        .withColumn(
            "call_datetime",
            F.coalesce(
                F.col("available_ts"),
                F.to_timestamp(F.col("call_date")),   # midnight at call_date
                F.col("load_dt")
            )
        )
    )

    # Normalize / cast fields (defensive casts even if Bronze typed them)
    df = (
        df
        .withColumn("call_number", F.col("call_number").cast("long"))
        .withColumn("incident_number", F.col("incident_number").cast("string"))  # keep as string for safety in joins
        .withColumn("unit_id", F.col("unit_id").cast("string"))
        .withColumn("city", F.initcap(F.trim(F.col("city"))))
        .withColumn("calltype", F.initcap(F.trim(F.col("calltype"))))
        .withColumn("call_type_group", F.initcap(F.trim(F.col("call_type_group"))))
        .withColumn("station_area", F.upper(F.regexp_replace(F.col("station_area"), r"\s+", "")))
        .withColumn("zipcode_of_incident", F.col("zipcode_of_incident").cast("int"))
        .withColumn("numalarms", F.col("numalarms").cast("int"))
        .withColumn("als_unit", F.col("als_unit").cast("boolean"))
        .withColumn("delay", F.col("delay").cast("double"))
        .withColumn("response_minutes", F.col("delay"))  # dataset-provided minutes
        # Derived calendar fields
        .withColumn("year", F.year("call_datetime"))
        .withColumn("month", F.month("call_datetime"))
        .withColumn("day", F.dayofmonth("call_datetime"))
        # Medical flag from either field
        .withColumn(
            "is_medical",
            (F.lower(F.coalesce(F.col("calltype"), F.lit(""))).like("%medical%")) |
            (F.lower(F.coalesce(F.col("call_type_group"), F.lit(""))).like("%medical%"))
        )
    )

    # Deduplicate: keep latest per call_number by call_datetime then load_dt
    w = Window.partitionBy("call_number").orderBy(
        F.col("call_datetime").desc_nulls_last(),
        F.col("load_dt").desc_nulls_last()
    )
    df = (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )

    # Audit column
    df = df.withColumn("_silver_processed_at", F.current_timestamp())

    return df

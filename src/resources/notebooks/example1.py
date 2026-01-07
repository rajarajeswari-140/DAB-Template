# Databricks notebook source
df=(spark.read
            .format("csv")
            .option("header","true")
            .option("interschema","true")
            .load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"))
df.count()
from pyspark.sql import functions as F

null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])
null_counts.show()

# COMMAND ----------


duplicate_count_all = df.count() - df.dropDuplicates().count()
print(f"Number of fully duplicate rows: {duplicate_count_all}")

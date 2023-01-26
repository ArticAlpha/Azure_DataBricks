# Databricks notebook source
df=spark.read.csv("/FileStore/tables/countries.csv",header=True , inferSchema=True)

# COMMAND ----------

## writing a CSV file into a parquet file.
## Note it doesn't require header=true options because Parquet files have columns and data types in them
df.write.parquet("/FileStore/tables/output/Countries_Parquet")

# COMMAND ----------



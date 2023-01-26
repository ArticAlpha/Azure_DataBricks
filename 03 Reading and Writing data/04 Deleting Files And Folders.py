# Databricks notebook source
## deleting the files and folder that are present in the DBFS
dbutils.fs.rm("/FileStore/tables/countries.txt")

# COMMAND ----------

## deleting json files 
dbutils.fs.rm("/FileStore/tables/countries_single_line.json")
dbutils.fs.rm("/FileStore/tables/countries_multi_line.json")

# COMMAND ----------

## deleting folders 
## Note the folders that have files in them will require additional parameter so that it can be deleted
dbutils.fs.rm("/FileStore/tables/output" , recurse=True)
dbutils.fs.rm("/FileStore/tables/countries_out",recurse=True)

# COMMAND ----------



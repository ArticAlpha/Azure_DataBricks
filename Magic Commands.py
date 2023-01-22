# Databricks notebook source
print('Hello')

# COMMAND ----------

# MAGIC %md
# MAGIC this is a text

# COMMAND ----------

# MAGIC %md
# MAGIC # this is a  header with single (#)
# MAGIC ## this is a header with two (#)
# MAGIC ### this is a header with three(#)

# COMMAND ----------

# MAGIC %sql
# MAGIC select 1

# COMMAND ----------

# MAGIC %md 
# MAGIC ### below is the demo based upon dbutils

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help

# COMMAND ----------

## listing directory
dbutils.fs.ls('/')

# COMMAND ----------

## listing a particular directory
dbutils.fs.ls('dbfs:/FileStore/')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------



# Databricks notebook source
countries_df = spark.read.options(header=True).csv("/FileStore/tables/countries.csv")

# COMMAND ----------

countries_df.show()

# COMMAND ----------

from pyspark.sql.types import StructType , StructField , IntegerType , StringType, DoubleType

countries_schema = StructType([
    StructField("COUNTRY_ID",IntegerType(),False),
    StructField("NAME", StringType(), False),
    StructField("NATIONALITY", StringType(), False),
    StructField("COUNTRY_CODE",StringType(), False),
    StructField("ISO_ALPHA2", StringType(), False),
    StructField("CAPITAL",StringType(),False),
    StructField("POPULATION",DoubleType(),False),
    StructField("AREA_KM2",IntegerType(),False),
    StructField("REGION_ID",IntegerType(),True),
    StructField("SUB_REGION_ID",IntegerType(),True),
    StructField("INTERMEDIATE_REGION_ID",IntegerType(),True),
    StructField("ORGANIZATION_REGION_ID",IntegerType(),True)
])


# COMMAND ----------

countries_df.dtypes

# COMMAND ----------

countries_df = spark.read.options(header=True).schema(countries_schema).csv("/FileStore/tables/countries.csv")

# COMMAND ----------

countries_df.dtypes

# COMMAND ----------

##writing Data using an existing csv file it will have three or more files in the countries_out folder log,csv and one more
countries_df.write.csv("/FileStore/tables/countries_out", header=True)

# COMMAND ----------

df=spark.read.csv("/FileStore/tables/countries_out",header=True)

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.csv("/FileStore/tables/output/countries_out",header=True)

# COMMAND ----------

## overwriting the data
df.write.csv("/FileStore/tables/output/countries_out",header=True,mode='overwrite')

# COMMAND ----------

## different ways of overwriting the data
df.write.options(header=True).mode('overwrite').csv("/FileStore/tables/output/countries_out")

# COMMAND ----------

## partitioning data and writing them in the folder
df.show()

# COMMAND ----------

df1.display()

# COMMAND ----------

## data is Partioned based upon REGION ID
df1.write.options(header=True).mode('overwrite').partitionBy('REGION_ID').csv("/FileStore/tables/output/countries_output")

# COMMAND ----------

## to see the data that have been partioned just read the folder 
df2=spark.read.csv("/FileStore/tables/output/countries_output",header=True)

# COMMAND ----------

df2.display()

# COMMAND ----------

## if you want to see partioned data go inside the folder and select the partiton
df3 = spark.read.csv("/FileStore/tables/output/countries_output/REGION_ID=10",header=True)

# COMMAND ----------

df3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### we can do partitioning based on multiple columns, folders generated will be nested.

# COMMAND ----------



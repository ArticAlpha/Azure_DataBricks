# Databricks notebook source
Countries_Path = '/FileStore/tables/countries.csv'

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

countries_dt=spark.read.csv(path=Countries_Path,header=True)

# COMMAND ----------

## using cast to change dara types
countries_dt.select(countries_dt['Population'].cast(IntegerType())).dtypes

# COMMAND ----------

## we can change as many columns as we want using the cast function

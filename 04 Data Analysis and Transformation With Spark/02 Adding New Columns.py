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

countries=spark.read.csv(path=Countries_Path, schema=countries_schema,header=True)

# COMMAND ----------

## adding a column current date
from pyspark.sql.functions import current_date

countries.withColumn('Current_Date',current_date()).display()

# COMMAND ----------

## using lit  -- it basically gives some default values
from pyspark.sql.functions import lit

countries.withColumn('Updated_By',lit('MVT')).display()

# COMMAND ----------

countries.withColumn('Population_in_Millions',countries['POPULATION']/1000000).display()

# COMMAND ----------



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

countries=spark.read.csv(path=Countries_Path, header=True, schema=countries_schema)

# COMMAND ----------

countries.display()

# COMMAND ----------

## doesn't require column names to be case sensitive, but you can't use methods
countries.select('COUNTRY_ID','name','NATIONALITY').display()

# COMMAND ----------

## require column names to be case sensitive and you can alse use methods
countries.select(countries.COUNTRY_ID,countries.NAME, countries.NATIONALITY).display()

# COMMAND ----------

## third way of selecting columns
countries.select(countries['country_id'],countries['name'],countries['nationality']).display()

# COMMAND ----------

## fourth way of selecting the columns
from pyspark.sql.functions import col

countries.select(col('NAME'), col('country_ID')).display()


# COMMAND ----------

## using alias
countries.select(countries['NAME'].alias('Country_Name'),countries['CAPITAL'].alias('Capital_City'),countries['POPULATION']).display()

# COMMAND ----------

## using withColumnRenamed option to rename column
countries.select('name','capital','nationality').withColumnRenamed('name','Country_Name').withColumnRenamed('capital','Capital_City').display()
## in this we didn't renamed the last column

# COMMAND ----------

## using region file
Regions_Path='/FileStore/tables/country_regions.csv'

Regions_Schema = StructType([
                 StructField('ID',StringType(),False),
                 StructField('NAME',StringType(),False)
])

regions=spark.read.csv(path=Regions_Path, schema=Regions_Schema , header=True)

# COMMAND ----------

regions.select('ID',regions['name'].alias('Continent')).display()

# COMMAND ----------



# Databricks notebook source
spark.read.csv("/FileStore/tables/countries.csv")

# COMMAND ----------

countries_df = spark.read.csv("/FileStore/tables/countries.csv")

# COMMAND ----------

countries_df.show()

# COMMAND ----------

countries_df.display()

# COMMAND ----------

display(countries_df)

# COMMAND ----------

## how to show column header
countries_df=spark.read.csv("/FileStore/tables/countries.csv" , header = True)

# COMMAND ----------

countries_df.show()

# COMMAND ----------

## another way to add header
countries_df = spark.read.options(header = True).csv("/FileStore/tables/countries.csv")

# COMMAND ----------

display(countries_df)

# COMMAND ----------

## check data types of the attached files
countries_df.dtypes

# COMMAND ----------

## you can also check it with
countries_df.schema

# COMMAND ----------

## third way of seeing the schema
countries_df.describe()

# COMMAND ----------

## using inferrSchema optionw we can see specify the schema automatically
countries_df= spark.read.options(header=True , inferSchema= True).csv("/FileStore/tables/countries.csv")

## its not very efficient because it uses two spark jobs one to infers schema and one to read the file.

# COMMAND ----------

## we can declare the data types/schema manually
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

countries_df = spark.read.csv("/FileStore/tables/countries.csv" , header=True , schema=countries_schema)

# COMMAND ----------

## another way to do it
countries_df = spark.read.options(header=True).schema(countries_schema).csv("/FileStore/tables/countries.csv")

# COMMAND ----------

 #################################################################################################################################################################
## reading  single line Json file
countries_json_sl_df = spark.read.json("/FileStore/tables/countries_single_line.json")

# COMMAND ----------

countries_json_sl_df.display()

# COMMAND ----------

## reading multi line json file, we need multiline as true to read a json file which is multiline
countries_json_ml_df = spark.read.json("/FileStore/tables/countries_multi_line.json", multiLine=True)

# COMMAND ----------

countries_json_ml_df.display()

# COMMAND ----------

## reading a tab delimeted file add \t in sep to read a tab delimeted file
countries_tab_df = spark.read.options(header = True, sep="\t").csv("/FileStore/tables/countries.txt")

# COMMAND ----------

countries_tab_df.display()

# COMMAND ----------



# Databricks notebook source
# MAGIC %md
# MAGIC # Data Access

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.nyctaxistoragehuym.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.nyctaxistoragehuym.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.nyctaxistoragehuym.dfs.core.windows.net", "11a8a387-57ba-4127-9e06-e32ae0105b24")
spark.conf.set("fs.azure.account.oauth2.client.secret.nyctaxistoragehuym.dfs.core.windows.net", "YkF8Q~Wf6NvmVD._cIymA6jA20~brWyiaJc04dBR")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.nyctaxistoragehuym.dfs.core.windows.net", "https://login.microsoftonline.com/b1a9fdc0-1d56-4c3d-a481-809fff8a26db/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Database

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE gold

# COMMAND ----------

# MAGIC %md
# MAGIC # Data reading and writing and creating delta tables

# COMMAND ----------

dbutils.fs.ls("abfss://silver@nyctaxistoragehuym.dfs.core.windows.net/")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Storage Variables**

# COMMAND ----------

silver_dir = 'abfss://silver@nyctaxistoragehuym.dfs.core.windows.net'
gold_dir = 'abfss://gold@nyctaxistoragehuym.dfs.core.windows.net'


# COMMAND ----------

# MAGIC %md
# MAGIC ### Data zone 

# COMMAND ----------

df_zone = spark.read.format('parquet')\
                    .option('inferSchema', 'true')\
                    .option('header', 'true')\
                    .load(f'{silver_dir}/trip_zone')                    
df_zone.display()

# COMMAND ----------

df_zone.write.format('delta')\
            .mode('append')\
            .option('path',f'{gold_dir}/trip_zone')\
            .saveAsTable('gold.trip_zone')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_zone
# MAGIC where Borough = 'EWR'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trip_type

# COMMAND ----------

df_type = spark.read.format('parquet')\
                    .option('inferSchema', 'true')\
                    .option('header', 'true')\
                    .load(f'{silver_dir}/trip_type')                    
df_type.display()

# COMMAND ----------

df_type.write.format('delta')\
            .mode('append')\
            .option('path',f'{gold_dir}/trip_type')\
            .saveAsTable('gold.trip_type')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Trip data

# COMMAND ----------

df_trip = spark.read.format('parquet')\
                    .option('inferSchema', 'true')\
                    .option('header', 'true')\
                    .load(f'{silver_dir}/trips2023data')                    
df_trip.display()

# COMMAND ----------

df_trip.write.format('delta')\
            .mode('append')\
            .option('path',f'{gold_dir}/trips2023data')\
            .saveAsTable('gold.trip')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip

# COMMAND ----------

# MAGIC %md
# MAGIC # Learning Delta lake

# COMMAND ----------

# MAGIC %md
# MAGIC ### Versioning

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_zone where LocationID = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC update gold.trip_zone set Borough = 'EMR' where LocationID = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from gold.trip_zone where LocationID = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history gold.trip_zone

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Time travel

# COMMAND ----------

# MAGIC %sql
# MAGIC restore gold.trip_zone to version as of 0

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_zone

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Tables

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip type**

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_type

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip zone**

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_zone

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip_data_2023**

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip

# COMMAND ----------


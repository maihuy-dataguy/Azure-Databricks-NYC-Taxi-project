# Databricks notebook source
# MAGIC %md
# MAGIC #Data access

# COMMAND ----------

application_id= "11a8a387-57ba-4127-9e06-e32ae0105b24"
tenant_id = "b1a9fdc0-1d56-4c3d-a481-809fff8a26db" 
secret = "6d18cbbb-1854-4c11-b1b1-52ad88153b1b"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.nyctaxistoragehuym.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.nyctaxistoragehuym.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.nyctaxistoragehuym.dfs.core.windows.net", "11a8a387-57ba-4127-9e06-e32ae0105b24")
spark.conf.set("fs.azure.account.oauth2.client.secret.nyctaxistoragehuym.dfs.core.windows.net", "YkF8Q~Wf6NvmVD._cIymA6jA20~brWyiaJc04dBR")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.nyctaxistoragehuym.dfs.core.windows.net", "https://login.microsoftonline.com/b1a9fdc0-1d56-4c3d-a481-809fff8a26db/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@nyctaxistoragehuym.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reader

# COMMAND ----------

# MAGIC %md
# MAGIC **Importing libraries**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading CSV data**

# COMMAND ----------

# MAGIC %md
# MAGIC **1. Trip_type data**

# COMMAND ----------

df_trip_type = spark.read.format("csv")\
                        .option("header", "true")\
                        .option("inferSchema", "true")\
                        .load("abfss://bronze@nyctaxistoragehuym.dfs.core.windows.net/trip_type")

# COMMAND ----------

df_trip_type.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **2.Trip_zone data**

# COMMAND ----------

df_trip_zone = spark.read.format("csv")\
                    .option("header", "true")\
                    .option("inferSChema","true")\
                    .load("abfss://bronze@nyctaxistoragehuym.dfs.core.windows.net/trip_zone")

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **3. Trip data**

# COMMAND ----------

df_trip = spark.read.format("parquet")\
                .option("header", "true")\
                .schema(schema)\
                .option('recursiveFileLookup', 'true')\
                .load("abfss://bronze@nyctaxistoragehuym.dfs.core.windows.net/trips2023_data")

# COMMAND ----------

schema = '''
                VendorID BIGINT,
                lpep_pickup_datetime TIMESTAMP,
                lpep_dropoff_datetime TIMESTAMP,
                store_and_fwd_flag STRING,
                RatecodeID BIGINT,
                PULocationID BIGINT,
                DOLocationID BIGINT,
                passenger_count BIGINT,
                trip_distance DOUBLE,
                fare_amount DOUBLE,
                extra DOUBLE,
                mta_tax DOUBLE,
                tip_amount DOUBLE,
                tolls_amount DOUBLE,
                ehail_fee DOUBLE,
                improvement_surcharge DOUBLE,
                total_amount DOUBLE,
                payment_type BIGINT,
                trip_type BIGINT,
                congestion_surcharge DOUBLE

      '''

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC **Taxi trip type**

# COMMAND ----------

df_trip_type = df_trip_type.withColumnRenamed('description', 'trip_description')
df_trip_type.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip zone**

# COMMAND ----------

df_trip_zone = df_trip_zone\
                .withColumn('zone_1',split(col('Zone'),'/').getItem(0))\
                .withColumn('zone_2',split(col('Zone'),'/').getItem(1))
df_trip_zone.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip data**
# MAGIC

# COMMAND ----------

df_trip = df_trip.withColumn('trip_date',to_date(col('lpep_pickup_datetime')))\
                .withColumn('trip_month',month(col('lpep_pickup_datetime')))\
                .withColumn('trip_year',year(col('lpep_pickup_datetime')))

# COMMAND ----------

df_trip = df_trip.select('VendorID','PULocationID','DOLocationID','fare_amount','total_amount')
df_trip.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Writer

# COMMAND ----------

df_trip_type.write.mode('append')\
              .format('parquet')\
              .option('path','abfss://silver@nyctaxistoragehuym.dfs.core.windows.net/trip_type')\
              .save()

# COMMAND ----------

df_trip_zone.write.mode('append')\
                .format('parquet')\
                .option('path','abfss://silver@nyctaxistoragehuym.dfs.core.windows.net/trip_zone')\
                .save()

# COMMAND ----------

df_trip.write.mode('append')\
                .format('parquet')\
                .option('path','abfss://silver@nyctaxistoragehuym.dfs.core.windows.net/trips2023data')\
                .save()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Analysis

# COMMAND ----------

display(df_trip)

# COMMAND ----------


# Databricks notebook source
# DBTITLE 1,Import Libs
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame

# COMMAND ----------

# DBTITLE 1,Load Circuits
df_circuits = spark.read.format("delta").table("analytics_f1_bronze.circuits")
df_circuits = df_circuits\
    .select(
        col("circuitId").alias("circuit_id"),
        col("circuitRef").alias("circuit_ref"),
        col("name"),
        col("location"),
        col("country"),
        col("lat").alias("latitude"),
        col("lng").alias("longitude"),
        col("alt").alias("altitude"))\
    .withColumn("ingestion_date", current_timestamp())
df_circuits.write.format("delta").mode("overwrite").saveAsTable("analytics_f1_silver.circuits")
        

# COMMAND ----------

# DBTITLE 1,Load Race
df_race = spark.read.format("delta").table("analytics_f1_bronze.races")
df_race = df_race\
    .withColumn("ingestion_date", current_timestamp())\
    .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"))\
    .select(
        col("raceId").alias("race_id"),
        col("year").alias("race_year"),
        col("round"),
        col("circuitId").alias("circuit_id"),
        col("name"),
        col("race_timestamp"),
        col("ingestion_date"))

df_race.write.format("delta").mode("overwrite").saveAsTable("analytics_f1_silver.races")

# COMMAND ----------

# DBTITLE 1,Load Constructors
df_constructors = spark.read.format("delta").table("analytics_f1_bronze.constructors")

df_constructors = df_constructors\
    .select(
        col("constructorId").alias("costructor_id"),
        col("constructorRef").alias("constructor_ref"),
        col("name"),
        col("nationality"))\
    .withColumn("ingestion_date", current_timestamp())
df_constructors.write.format("delta").mode("overwrite").saveAsTable("analytics_f1_silver.constructors")
    

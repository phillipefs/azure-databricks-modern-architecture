# Databricks notebook source
# DBTITLE 1,Circuits_Silver
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame

# COMMAND ----------

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
    .withColumn("ingestion_date", current_timestamp()).show()
        

# COMMAND ----------

display(df_circuits)

# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

def add_ingestion_date(df_input):
    df_output = df_input.withColumn("ingestion_date", current_timestamp())
    return df_output   

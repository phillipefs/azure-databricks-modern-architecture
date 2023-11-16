# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Create Databases and Delta Tables
# MAGIC > *DB and Delta Tables*
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Databases
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS analytics_f1_bronze LOCATION "/mnt/layer-bronze/databricks/analytics_f1_bronze";
# MAGIC CREATE DATABASE IF NOT EXISTS analytics_f1_silver LOCATION "/mnt/layer-silver/databricks/analytics_f1_silver";
# MAGIC CREATE DATABASE IF NOT EXISTS analytics_f1_gold   LOCATION "/mnt/layer-gold/databricks/analytics_f1_gold";

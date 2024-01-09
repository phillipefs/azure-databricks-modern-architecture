# Databricks notebook source
# DBTITLE 1,Run Notebook Bronze
dbutils.notebook.run("process_bronze", 0)

# COMMAND ----------

# DBTITLE 1,Run Notebook Silver
dbutils.notebook.run("process_silver", 0, arguments={"db_bronze":"analytics_f1_bronze", "db_silver":"analytics_f1_silver"})


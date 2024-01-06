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
    

# COMMAND ----------

# DBTITLE 1,Load Drivers
df_drives = spark.read.format("delta").table("analytics_f1_bronze.drivers")

df_drivers = df_drives\
  .select(
      regexp_replace(col("code"), "\\\\N", "").alias("code"),
      col("dob").cast("date"),
      col("driverId").alias("drive_id"),
      col("driverRef").alias("drive_ref"),
      concat(col("name.forename"), lit(" "), col("name.surname")).alias("name"),
      col("nationality"),
      regexp_replace(col("number"), "\\\\N", "").cast("int").alias("number"))\
   .replace("", None)

df_drives.write.format("delta").mode("overwrite").saveAsTable("analytics_f1_silver.drivers")


# COMMAND ----------

# DBTITLE 1,Load Results
df_results = spark.read.format("delta").table("analytics_f1_bronze.results")

df_results = df_results.select(
    col("resultId").alias("result_id"),
    col("raceId").alias("race_id"),
    col("driverId").alias("driver_id"),
    col("constructorId").alias("constructor_id"),
    col("fastestLap").cast("int").alias("fastest_lap"),
    col("fastestLapSpeed").cast("float").alias("fastest_lap_speed"),
    col("fastestLapTime").alias("fastest_lap_time"),
    col("grid"),
    col("laps"),
    col("milliseconds").cast("long"),
    col("number"),
    col("points"),
    col("position").cast("int"),
    col("positionOrder").alias("position_order"),
    col("positionText").alias("position_text"),
    col("rank").cast("int"))
df_results.write.format("delta").mode("overwrite").saveAsTable("analytics_f1_silver.results")

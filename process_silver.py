# Databricks notebook source
# DBTITLE 1,Import Libs
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("db_bronze", "")
dbutils.widgets.text("db_silver", "")
db_bronze = dbutils.widgets.get("db_bronze")
db_silver = dbutils.widgets.get("db_silver")

# COMMAND ----------

# DBTITLE 1,Import Common Functions
# MAGIC %run ./utils/common_functions

# COMMAND ----------

# DBTITLE 1,Load Circuits
df_circuits = spark.read.format("delta").table(f"{db_bronze}.circuits")
df_circuits = df_circuits\
    .select(
        col("circuitId").alias("circuit_id"),
        col("circuitRef").alias("circuit_ref"),
        col("name"),
        col("location"),
        col("country"),
        col("lat").alias("latitude"),
        col("lng").alias("longitude"),
        col("alt").alias("altitude"))

df_circuits = add_ingestion_date(df_circuits)
df_circuits.write.format("delta").mode("overwrite").saveAsTable(f"{db_silver}.circuits")
        

# COMMAND ----------

# DBTITLE 1,Load Race
df_race = spark.read.format("delta").table(f"{db_bronze}.races")
df_race = df_race\
    .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"))\
    .select(
        col("raceId").alias("race_id"),
        col("year").alias("race_year"),
        col("round"),
        col("circuitId").alias("circuit_id"),
        col("name"),
        col("race_timestamp"))
    
df_race = add_ingestion_date(df_race)
df_race.write.format("delta").mode("overwrite").saveAsTable(f"{db_silver}.races")

# COMMAND ----------

# DBTITLE 1,Load Constructors
df_constructors = spark.read.format("delta").table(f"{db_bronze}.constructors")

df_constructors = df_constructors\
    .select(
        col("constructorId").alias("costructor_id"),
        col("constructorRef").alias("constructor_ref"),
        col("name"),
        col("nationality"))
    
df_constructors = add_ingestion_date(df_constructors)
df_constructors.write.format("delta").mode("overwrite").saveAsTable(f"{db_silver}.constructors")
    

# COMMAND ----------

# DBTITLE 1,Load Drivers
df_drives = spark.read.format("delta").table(f"{db_bronze}.drivers")

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

df_drivers = add_ingestion_date(df_drivers)
df_drives.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(f"{db_silver}.drivers")


# COMMAND ----------

# DBTITLE 1,Load Results
df_results = spark.read.format("delta").table(f"{db_bronze}.results")

df_results = df_results\
    .select(
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

df_results = add_ingestion_date(df_results)
df_results.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(f"{db_silver}.results")

# COMMAND ----------

# DBTITLE 1,Load Pit Stops
df_pit_stops = spark.read.format("delta").table(f"{db_bronze}.pit_stops")

df_pit_stops\
    .select(
        col("driverId").alias("driver_id"),
        col("duration"),
        col("lap"),
        col("milliseconds"),
        col("raceId").alias("race_id"),
        col("stop"),
        col("time"))

df_pit_stops = add_ingestion_date(df_pit_stops)
df_pit_stops.write.format("delta").mode("overwrite").saveAsTable(f"{db_silver}.pit_stops")

# COMMAND ----------

# DBTITLE 1,Load Lap Times
df_lap_times = spark.read.format("delta").table(f"{db_bronze}.lap_times")
df_lap_times = df_lap_times\
    .select(
        col("raceId").alias("race_id"),
        col("driverId").alias("drive_id"),
        col("lap"),
        col("position"),
        col("time"),
        col("milliseconds"))

df_lap_times = add_ingestion_date(df_lap_times)
df_lap_times.write.format("delta").mode("overwrite").saveAsTable(f"{db_silver}.lap_times")


# COMMAND ----------

# DBTITLE 1,Load Qualifying
df_qualifying = spark.read.format("delta").table(f"{db_bronze}.qualifying")

df_qualifying\
    .select(
        col("constructorId").cast("int").alias("constructor_id"),
        col("driverId").cast("int").alias("driver_id"),
        col("qualifyId").cast("int").alias("qualify_id"),
        col("raceId").cast("int").alias("race_id"),
        col("number").cast("int").alias("number"),
        col("position").cast("int").alias("position"),
        col("q1"),
        col("q2"),
        col("q3"))

df_qualifying = add_ingestion_date(df_qualifying)
df_qualifying.write.format("delta").mode("overwrite").saveAsTable(f"{db_silver}.qualifying")

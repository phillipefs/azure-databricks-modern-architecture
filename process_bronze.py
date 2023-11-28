# Databricks notebook source
# DBTITLE 1,Import Class Ingestion
# MAGIC %run ./utils/class_ingestion_bronze

# COMMAND ----------

# DBTITLE 1,List Files 
list_files = [
    {
        "dir_file": "/mnt/layer-bronze/udemy-databricks/raw/constructors.json",
        "file_format": "json",
        "table_name": "constructors",
        "keys_merge": ["constructorId"],
    },
    {
        "dir_file": "/mnt/layer-bronze/udemy-databricks/raw/circuits.csv",
        "file_format": "csv",
        "options": {"header": True, "inferSchema": True},
        "table_name": "circuits",
        "keys_merge": ["circuitId"]
    },
    {
        "dir_file": "/mnt/layer-bronze/udemy-databricks/raw/races.csv",
        "file_format": "csv",
        "options": {"header": True, "inferSchema": True},
        "table_name": "races",
        "keys_merge": ["raceId"]
    },
    {
        "dir_file": "/mnt/layer-bronze/udemy-databricks/raw/drivers.json",
        "file_format": "json",
        "table_name": "drivers",
        "keys_merge": ["driverId"],
    },
    {
    "dir_file": "/mnt/layer-bronze/udemy-databricks/raw/pit_stops.json",
    "file_format": "json",
    "options": {"multiline": True,},
    "table_name": "pit_stops",
    "keys_merge": ["raceId","driverId", "stop"],
    },
    {
    "dir_file": "/mnt/layer-bronze/udemy-databricks/raw/results.json",
    "file_format": "json",
    "table_name": "resuls",
    "keys_merge": ["resultId","raceId","driverId","constructorId"],
    }
]

# COMMAND ----------

# DBTITLE 1,Start Process
from concurrent.futures import ThreadPoolExecutor

def process_file(file_info):
    f1 = IngestionRawF1(
        spark=spark,
        dir_file=file_info.get("dir_file"),
        format_file=file_info.get("file_format"),
        table_name=file_info.get("table_name"),
        keys_merge=file_info.get("keys_merge"),
        options_file=file_info.get("options", {}),
        schema_file=file_info.get("schema")
    )
    f1.start_ingestion_raw()

with ThreadPoolExecutor(max_workers=4) as executor:
    executor.map(process_file, list_files)

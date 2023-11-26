# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame

# COMMAND ----------

class IngestionRawF1:
    
    def __init__(self, spark, dir_file:str, format_file:str, table_name:str, 
                 keys_merge:list, schema_file:str=None, options_file:dict=None):
        self.spark = spark
        self.dir_file = dir_file
        self.format_file = format_file
        self.table_name = table_name
        self.keys_merge = keys_merge
        self.schema_file = schema_file
        self.options_file = options_file if options_file is not None else {}

        self.database_raw = "analytics_f1_bronze"

    def df_raw(self) -> DataFrame:
        if self.format_file in self.dir_file:
            path_read = self.dir_file
        else:
            path_read = self.dir_file + f"/*.{self.format_file}"

        if self.schema_file:
            df_raw = self.spark.read.format(self.format_file).schema(self.schema_file).options(**self.options_file).load(path_read)
        else:
            df_raw = self.spark.read.format(self.format_file).options(**self.options_file).load(path_read)
        return df_raw

    def merge_into_raw(self, df_raw: DataFrame)-> None:
        if spark.catalog.tableExists(f"{self.database_raw}.{self.table_name}"):
            print(self.table_name)
            conditions_merge = " and ".join(f"source.{key} = target.{key}" for key in self.keys_merge)
            df_raw.createOrReplaceTempView("source")
            spark.sql(
                f"""
                MERGE INTO {self.database_raw}.{self.table_name} as target
                    USING source
                    on {conditions_merge}
                    WHEN MATCHED THEN 
                        UPDATE SET *
                    WHEN NOT MATCHED THEN
                        INSERT *
                """
            )
        else:
            df_raw.write.format("delta").saveAsTable(f"{self.database_raw}.{self.table_name}")

    def start_ingestion_raw(self)->None:
        df = self.df_raw()
        self.merge_into_raw(df)


# COMMAND ----------

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

for files in list_files:
    f1 = IngestionRawF1(
        spark      = spark,
        dir_file   = files.get("dir_file"),
        format_file= files.get("file_format"),
        table_name = files.get("table_name"),
        keys_merge = files.get("keys_merge"),
        options_file = files.get("options", {}),
        schema_file = files.get("schema")
    )
    f1.start_ingestion_raw()

# COMMAND ----------

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

# Lista de arquivos
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
    "table_name": "pit_stops",
    "keys_merge": ["raceId","driverId"],
    },
    {
    "dir_file": "/mnt/layer-bronze/udemy-databricks/raw/results.json",
    "file_format": "json",
    "table_name": "resuls",
    "keys_merge": ["resultId","raceId","driverId","constructorId"],
    }
]

# Iniciar threads para processar cada arquivo em paralelo usando ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=6) as executor:
    executor.map(process_file, list_files)


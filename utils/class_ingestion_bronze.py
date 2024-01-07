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
        try:
            if self.format_file in self.dir_file:
                path_read = self.dir_file
            else:
                path_read = self.dir_file + f"/*.{self.format_file}"

            if self.schema_file:
                df_raw = self.spark.read.format(self.format_file).schema(self.schema_file).options(**self.options_file).load(path_read)
            else:
                df_raw = self.spark.read.format(self.format_file).options(**self.options_file).load(path_read)
            
            return df_raw
        except Exception as e:
            print(f"Error in df_raw(): {str(e)}")

    def merge_into_raw(self, df_raw: DataFrame)-> None:
        try:
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
        except Exception as e:
            print(f"Error in merge_into_raw(): {str(e)}")

    def start_ingestion_raw(self)->None:
        df = self.df_raw()
        self.merge_into_raw(df)


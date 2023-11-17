# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

class IngestionRawF1:
    def __init__(self, spark):
        self.spark = spark
        self.database_raw = "teste"

    def df_raw(self, format_file, dir_file, options: dict = {}, schema: str = None):
        if format_file in dir_file:
            path_read = dir_file
        else:
            path_read = dir_file + f"/*.{format_file}"

        if schema:
            df_raw = self.spark.read.format(format_file).schema(schema).options(**options).load(path_read)
        else:
            df_raw = self.spark.read.format(format_file).options(**options).load(path_read)

        df_raw.show()


# COMMAND ----------

dbutils.fs.ls("/mnt/layer-bronze/udemy-databricks/raw")

# COMMAND ----------

# Instancia a classe IngestionRawF1
f1 = IngestionRawF1(spark)

# Define o esquema para o arquivo CSV
custom_schema = "id int, n int, n1 int, n2 int, number string, number2 string"

# Chama o método df_raw passando o esquema
f1.df_raw(
    format_file="json",
    dir_file="dbfs:/mnt/layer-bronze/udemy-databricks/raw/constructors.json",
    options={},
    schema=custom_schema
)

# COMMAND ----------

f1 = IngestionRawF1(spark)

# COMMAND ----------

list_files = [
    {
        "dir_file":"/mnt/layer-bronze/udemy-databricks/raw/circuits.csv",
        "file_format":"csv",
        "options":{"header":True, "inferSchema":True}
    },
    {
        "dir_file":"/mnt/layer-bronze/udemy-databricks/raw/drivers.json",
        "file_format":"json"
    },
]

# COMMAND ----------

for files in list_files:
    f1.df_raw(
        dir_file=    files.get("dir_file"),
        format_file= files.get("file_format"),
        options=     files.get("options", {}),
        schema=      files.get("schema")
    )

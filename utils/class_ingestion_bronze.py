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

    def merge_into_raw(self,df_raw):
        if spark.catalog.tableExists(f"{self.database_raw}.{self.table_name}"):
            df_raw.createOrReplaceTempView("source")
            spark.sql(
                f"""
                MERGE INTO {self.database_raw}.{self.table_name} as target
                    USING source
                    on source.constructorId = target.constructorId
                    WHEN MATCHED THEN 
                        UPDATE SET *
                    WHEN NOT MATCHED THEN
                        INSERT *
                """
            )
            print("Table Exist")
        else:
            df_raw.write.format("delta").saveAsTable(f"{self.database_raw}.{self.table_name}")

    def start_ingestion_raw(self)->None:
        df = self.df_raw()
        self.merge_into_raw(df)


# COMMAND ----------

f1 = IngestionRawF1(
    spark = spark,
    dir_file="dbfs:/mnt/layer-bronze/udemy-databricks/raw/constructors.json",
    format_file="json",
    table_name="constructors",
    keys_merge=None,
    options_file=None,
    schema_file=None
)
f1.start_ingestion_raw()

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

# COMMAND ----------

tables = spark.sql(f"show tables in airlines_bronze")

# COMMAND ----------

spark.catalog.tableExists("airlines_bronze.air_cia")

# COMMAND ----------

# Lista de elementos
elementos = ['id', 'cpf', 'nome', 'endereco', 'telefone']

# Criando a string de forma dinâmica
condicoes = " and ".join(f"source.{elemento} = target.{elemento}" for elemento in elementos)

# Exibindo a string resultante
print(condicoes)


# COMMAND ----------

print(" X ".join(list((f"source.{elemento} = target.{elemento}" for elemento in elementos))))

# COMMAND ----------

df_columns = ['id', 'cpf', 'nome', 'endereco', 'telefone']

column_string = ''
column_string_merge = ''
for value in df_columns:
    if value == df_columns[-1]:
        column_string += value
        column_string_merge += 'src.' + value
    else:
        column_string += value + ', '
        column_string_merge += 'src.' + value + ', '

print(column_string_merge)

# COMMAND ----------

df_estrutura = ['id', 'cpf', 'nome', 'endereco', 'telefone']
columns_merge = "id|cpf"

column_string = ''
column_string_merge = ''
for value in df_estrutura:
    if value == df_estrutura[-1]:
        column_string += value
        column_string_merge += 'src.' + value
    else:
        column_string += value + ', '
        column_string_merge += 'src.' + value + ', '
        
colunas, chave_fmt, flag = '', '', False
arr_chaves = columns_merge.split("|")
for x in df_estrutura:
    for chave in arr_chaves:            
        find = colunas.find(x)
        if chave != "" and flag == False:
            chave_fmt = f"{chave_fmt} and src.{chave} = dtn.{chave}"
            print(chave_fmt)
        elif chave != x and chave != '' and find == -1:  
            colunas += f'dtn.{x} = src.{x} ,'    
    flag = True
cond_merge = chave_fmt[5:]
colunas = colunas.replace(',dtn.update_date = src.update_date','')




# COMMAND ----------

df_columns = ['id', 'cpf', 'nome', 'endereco', 'telefone']
columns_merge = "id|cpf"

keys_columns = columns_merge.split("|")

print(", ".join(f"source.{column} = target.{column}" for column in keys_columns))

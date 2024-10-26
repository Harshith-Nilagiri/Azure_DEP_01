# Databricks notebook source
# MAGIC %md
# MAGIC B2S

# COMMAND ----------

input_path = 'dbfs:/mnt/bronze/SalesLT/Address/Address.parquet'

# COMMAND ----------

# Verify the path
input_path = "dbfs:/mnt/bronze/SalesLT/Address"
if not dbutils.fs.ls(input_path):
    raise FileNotFoundError(f"Path does not exist: {input_path}")

# Load the Parquet file
df = spark.read.format('parquet').load(input_path)
display(df)

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, date_format
from pyspark.sql.types import TimestampType

df = df.withColumn("ModifiedDate",date_format(from_utc_timestamp(df["ModifiedDate"].cast(TimestampType()),"UTC"),"yyyy-MM-dd"))

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC S2G
# MAGIC

# COMMAND ----------

dbutils.fs.ls('mnt/silver/SalesLT/')

# COMMAND ----------

dbutils.fs.ls('/mnt/gold/')

# COMMAND ----------

input_path = '/mnt/silver/SalesLT/Address/'

# COMMAND ----------

df = spark.read.format('delta').load(input_path)

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,regexp_replace

column_names =  df.columns
for old_col_name in column_names:
    new_col_name = "".join(["_" + char if char.isupper() and not old_col_name[i -1].isupper() else char for i, char in enumerate(old_col_name)]).lstrip("_")
    df = df.withColumnRenamed(old_col_name, new_col_name)

# COMMAND ----------

display(df)

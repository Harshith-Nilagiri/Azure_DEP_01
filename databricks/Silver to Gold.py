# Databricks notebook source
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

# COMMAND ----------

# MAGIC %md
# MAGIC Doing transformation for all tables(Changing column names)

# COMMAND ----------

table_names = []

for i in dbutils.fs.ls('mnt/silver/SalesLT/'):
    table_names.append(i.name.split('/')[0])

# COMMAND ----------

table_names

# COMMAND ----------

for table_name in table_names:
    path = '/mnt/silver/SalesLT/' + table_name
    print(path)
    df = spark.read.format('delta').load(path)

    column_names = df.columns

    for old_col_name in column_names:
        new_col_name = "".join(["_" + char if char.isupper() and not old_col_name[i -1].isupper() else char for i, char in enumerate(old_col_name)]).lstrip("_")

        df = df.withColumnRenamed(old_col_name, new_col_name)

    output_path = '/mnt/gold/SalesLT/' + table_name + '/'
    df.write.format('delta').mode('overwrite').save(output_path)


# COMMAND ----------

display(df)

# Databricks notebook source
# MAGIC %md
# MAGIC Doing Date transformation for all tables - Bronze to Silver

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, date_format
from pyspark.sql.types import TimestampType

# COMMAND ----------

table_names = []

for i in dbutils.fs.ls('mnt/bronze/SalesLT/'):
    table_names.append(i.name.split('/')[0])

# COMMAND ----------

for table_name in table_names:
    path = '/mnt/bronze/SalesLT/' + table_name + '/'
    df = spark.read.format('parquet').load(path)
    column = df.columns

    for col in column:
        if "Date" in col or "date" in col:
            df = df.withColumn(col,date_format(from_utc_timestamp(df[col].cast(TimestampType()),"UTC"),"yyyy-MM-dd"))
    
    output_path = '/mnt/silver/SalesLT/' + table_name + '/'
    df.write.format('delta').mode("overwrite").save(output_path)

# COMMAND ----------

display(df)

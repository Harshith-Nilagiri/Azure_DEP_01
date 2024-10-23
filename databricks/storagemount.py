# Databricks notebook source
application_id = dbutils.secrets.get(scope="p1-secret-scope",key="ar-client-id")

# COMMAND ----------

tenant_id = dbutils.secrets.get(scope="p1-secret-scope",key="ar-tenant-id")

# COMMAND ----------

client_secret = dbutils.secrets.get(scope="p1-secret-scope",key="ar-client-secret")

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": f"{application_id}",
    "fs.azure.account.oauth2.client.secret": f"{client_secret}",
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
    source = "abfss://bronze@dep1datalekegen21.dfs.core.windows.net/",
    mount_point = "/mnt/bronze",
    extra_configs = configs
)

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze/SalesLT")

# COMMAND ----------

dbutils.fs.mount(
    source = "abfss://silver@dep1datalekegen21.dfs.core.windows.net/",
    mount_point = "/mnt/silver",
    extra_configs = configs
)

# COMMAND ----------

dbutils.fs.mount(
    source = "abfss://gold@dep1datalekegen21.dfs.core.windows.net/",
    mount_point = "/mnt/gold",
    extra_configs = configs
)

# COMMAND ----------

dbutils.fs.ls('mnt/silver')

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
# MAGIC Doing transformation for all tables

# COMMAND ----------

table_names = []

for i in dbutils.fs.ls('mnt/bronze/SalesLT/'):
    table_names.append(i.name.split('/')[0])

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, date_format
from pyspark.sql.types import TimestampType

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

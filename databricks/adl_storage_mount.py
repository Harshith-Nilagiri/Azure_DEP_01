# Databricks notebook source
application_id = dbutils.secrets.get(scope="p1-secret-scope",key="ar-client-id")
tenant_id = dbutils.secrets.get(scope="p1-secret-scope",key="ar-tenant-id")
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

dbutils.fs.mount(
    source = "abfss://gold@dep1datalekegen21.dfs.core.windows.net/",
    mount_point = "/mnt/gold",
    extra_configs = configs
)

# COMMAND ----------

dbutils.fs.ls('mnt/silver')

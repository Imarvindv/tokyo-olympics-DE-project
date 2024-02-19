# Databricks notebook source
client_id = dbutils.secrets.get(scope='olympics-scope', key='olympics-app-client-id')
tenant_id = dbutils.secrets.get(scope='olympics-scope', key='olympics-app-tenant-id')
client_secret = dbutils.secrets.get(scope='olympics-scope', key='olympics-app-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": client_id,
           "fs.azure.account.oauth2.client.secret": client_secret,
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://raw-data@tokyoolympics1.dfs.core.windows.net/",
  mount_point = "/mnt/tokyoolympics1/raw-data",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls("/mnt/tokyoolympics1/raw-data"))

# COMMAND ----------

display(spark.read.csv('/mnt/tokyoolympics1/raw-data/athlete/athlete.csv'))

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://processed@tokyoolympics1.dfs.core.windows.net/",
  mount_point = "/mnt/tokyoplympics1/processed",
  extra_configs = configs)
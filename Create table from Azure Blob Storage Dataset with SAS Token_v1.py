# Databricks notebook source
# DBTITLE 1,Install dependencies
# MAGIC %pip install azure-storage-blob

# COMMAND ----------

# DBTITLE 1,Restart python interpreter
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Define Constants
ACCOUNT_NAME = "" # replace with your storage account name
SECRET_NAME = "" # replace with your secret name
SECRET_SCOPE = "" # replace with your secret scope
ACCOUNT = f"https://{ACCOUNT_NAME}.blob.core.windows.net/"
SAS_TOKEN = dbutils.secrets.get(SECRET_SCOPE, SECRET_NAME)
CONTAINER = "data" # replace with your container name
BLOB_NAME = "bing_covid-19_data.csv"

# COMMAND ----------

# DBTITLE 1,Import BlobServiceClient
from azure.storage.blob import BlobServiceClient

# COMMAND ----------

# DBTITLE 1,Create blob_service_client object
blob_service_client = BlobServiceClient(
        account_url=ACCOUNT,
        credential=SAS_TOKEN)

# COMMAND ----------

# DBTITLE 1,Create container_client object
container_client = blob_service_client.get_container_client(CONTAINER)

# COMMAND ----------

# DBTITLE 1,Create blob_client object
blob_client = container_client.get_blob_client(BLOB_NAME)

# COMMAND ----------

# DBTITLE 1,Read csv with pandas
import pandas as pd
df = pd.read_csv(blob_client.url, low_memory=False)

# COMMAND ----------

# DBTITLE 1,Convert pandas data frame to spark data frame
sdf = spark.createDataFrame(df)

# COMMAND ----------

# DBTITLE 1,Crate schema if it doesn't exists
spark.sql("CREATE SCHEMA IF NOT EXISTS covid")

# COMMAND ----------

# DBTITLE 1,Write data frame into a delta table
sdf.write.mode("overwrite").saveAsTable("covid.covid_data")

# COMMAND ----------

# DBTITLE 1,Descriptive statistics of the covid.covid_data table
spark.read.table("covid.covid_data").describe().display()

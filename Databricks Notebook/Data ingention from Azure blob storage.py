# Databricks notebook source
# MAGIC %md
# MAGIC # Data ingention from Azure blob storage

# COMMAND ----------

# Getting all details from Azure
storage_account_name = 'project99110'
storage_account_access_key = '4BfiXnNzQuMJD/xPYa8Q9TlyOKcYzt86OMZ3xFBNuj5DZzppzZAVby4xN/cT/cXRy+J345wHXTpy+AStOtjdig=='
blob_container = 'hotel-datasets'

# COMMAND ----------

spark.conf.set('fs.azure.account.key.' + storage_account_name + '.blob.core.windows.net', storage_account_access_key)

# COMMAND ----------

filePath = "wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/hotel_bookings_raw.csv.zip/"

# COMMAND ----------

# Display the data from azure Gen2
display(dbutils.fs.ls(filePath))

# COMMAND ----------

# Loading the datasets
datasets = spark.read.csv(
                                "wasbs://hotel-datasets@project99110.blob.core.windows.net/hotel_bookings_raw.csv.zip/hotel_bookings_raw.csv",
                                header=True,
                                inferSchema=True
                          )

# COMMAND ----------

# Create the duplicate copy of original datasets
df = datasets.alias('copy')

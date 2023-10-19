# Databricks notebook source
# MAGIC %run "/Repos/akashpandey15071996@outlook.com/end-to-end-azurePipeline-Hotel-Booking-project-with-PySpark/Databricks Notebook/Data_Processing"

# COMMAND ----------

# Specify the write mode as "overwrite"
df.write.mode("overwrite").csv("wasbs://hotel-datasets@project99110.blob.core.windows.net/processed_datasets/")

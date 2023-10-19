# Databricks notebook source
# MAGIC %md
# MAGIC #EDA Analysis on the datasets

# COMMAND ----------

# MAGIC %run "/Repos/akashpandey15071996@outlook.com/end-to-end-azurePipeline-Hotel-Booking-project-with-PySpark/Databricks Notebook/Data_Processing"

# COMMAND ----------

df.columns

# COMMAND ----------

df.count()

# COMMAND ----------

df.groupBy(col("hotel")).count().show()

# COMMAND ----------

df.groupBy(col("market_segment")).count().show()

# COMMAND ----------

df.groupBy(col("market_segment")).count().show()

# COMMAND ----------

df.display()

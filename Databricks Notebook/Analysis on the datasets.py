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

# MAGIC %md
# MAGIC
# MAGIC # Descriptive Statistics:
# MAGIC
# MAGIC * What are the summary statistics for numerical columns

# COMMAND ----------

df.describe().display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Categorical Features:

# COMMAND ----------

df.groupBy(col("hotel")).count().display()

# COMMAND ----------

df.groupBy(col("arrival_date_month")).count().display()

# COMMAND ----------

df.display()

# COMMAND ----------

df.groupBy(col("distribution_channel")).count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # What is the distribution of the "is_canceled" column, and is it related to cancellations or other variables?

# COMMAND ----------

df.groupBy(col("is_canceled")).count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Can you identify trends or patterns in the dataset based on the arrival_date_year and arrival_date_month columns?

# COMMAND ----------

df.groupBy(col("arrival_date_year"),col( "arrival_date_month")).count().show()

# COMMAND ----------

df.groupBy(col('arrival_date_month')).count().show()

# COMMAND ----------

df.groupBy(col("arrival_date_year")).count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Guest Demographics:
# MAGIC
# MAGIC * What can you infer about the demographics of guests based on columns like adults, children, and babies?
# MAGIC * Are there any interesting relationships or correlations between these variables?

# COMMAND ----------

df.groupBy(col("adults")).count().show()

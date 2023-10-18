# Databricks notebook source
# MAGIC %run "/Users/akashpandey15071996@outlook.com/Data ingention from Azure blob storage"

# COMMAND ----------

# Importing the important libraries
from pyspark.sql.functions import *

# COMMAND ----------

# show the columns
df.columns

# COMMAND ----------

# check the len of columns
len(df.columns)

# COMMAND ----------

# checking the datasize

df.count()

# COMMAND ----------

print(f"The Datasets is having {df.count()} rows and {len(df.columns)} columns.")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# To show the null values count of the datasets     
df.select([sum(col(i).isNull().cast('int')).alias(i) for i in df.columns]).display()

# COMMAND ----------

[i for i,v in df.dtypes if v in ['string']]

# COMMAND ----------

df.select([i for i,v in df.dtypes if v in ['string']]).display()

# COMMAND ----------



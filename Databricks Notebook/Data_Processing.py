# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Data_Processing

# COMMAND ----------

# MAGIC %run "/Repos/akashpandey15071996@outlook.com/end-to-end-azurePipeline-Hotel-Booking-project-with-PySpark/Databricks Notebook/Data ingention from Azure blob storage"

# COMMAND ----------

# Importing the important libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *

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

df.groupBy(col('CPI_AVG')).count().show()

# COMMAND ----------

df = df.withColumn("CPI_AVG", col("CPI_AVG").cast("double"))
df = df.withColumn("INFLATION_CHG",col("INFLATION_CHG").cast("double"))
df = df.withColumn("CSMR_SENT", col("CSMR_SENT").cast("double"))
df = df.withColumn("UNRATE", col("UNRATE").cast("double"))
df = df.withColumn("INTRSRT", col("INTRSRT").cast("double"))
df = df.withColumn("GDP", col("GDP").cast("double"))
df = df.withColumn("FUEL_PRCS", col("FUEL_PRCS").cast("double"))
df = df.withColumn("CPI_HOTELS", col("CPI_HOTELS").cast("double"))
df = df.withColumn("US_GINI", col("US_GINI").cast("double"))
df = df.withColumn("DIS_INC", col("DIS_INC").cast("double"))
df = df.withColumn("INFLATION", col("INFLATION").cast("double"))
df = df.withColumn("children", col("children").cast("double"))
df = df.withColumn("agent", (when(col("agent")== "NULL", "0").otherwise(col("agent"))).cast('double'))

# COMMAND ----------

[i for i,v in df.dtypes if v in ['string']]

# COMMAND ----------

display(df.select([i for i,v in df.dtypes if v in ['string']]))

# COMMAND ----------

df.select("reservation_status_date").show()

# COMMAND ----------

df.groupBy(col("reservation_status_date")).count().show()

# COMMAND ----------

df = df.withColumn("reservation_status_date", to_date(df["reservation_status_date"], "M/d/yyyy"))

# COMMAND ----------

df.select("reservation_status_date").show()

# COMMAND ----------

df.select([i for i,v in df.dtypes if v in ['string']]).display()

# COMMAND ----------


[i for i,v in df.dtypes if v in ['int', 'double']]

# COMMAND ----------

display(df.select([i for i,v in df.dtypes if v in ['int', 'double']]))

# COMMAND ----------

[i for i,v in df.dtypes if v in ['date']]

# COMMAND ----------

display(df.select([i for i,v in df.dtypes if v in ['date']]))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.dropna()

# COMMAND ----------

df.count()

# COMMAND ----------

# Loading the datasets
datasets = spark.read.csv(
                                "wasbs://hotel-datasets@project99110.blob.core.windows.net/hotel_bookings_raw.csv.zip/hotel_bookings_raw.csv",
                                header=True,
                                inferSchema=True
                          )

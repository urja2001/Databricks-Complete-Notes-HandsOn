# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Streaming Using Files

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the folder structure in DBFS File System

# COMMAND ----------

dbutils.fs.mkdirs("/FileStore/tables/stream_read/")
dbutils.fs.mkdirs("/FileStore/tables/stream_checkpoint/")
dbutils.fs.mkdirs("/FileStore/tables/stream_write/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Schema for Streaming Data

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *

# COMMAND ----------

schema_defined = StructType([StructField('File',StringType(),True),
                             StructField('Shop',StringType(),True),
                             StructField('Sale_count',IntegerType(),True)
                            ])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Streaming Data

# COMMAND ----------

df = spark.readStream.format("csv") \
    .schema(schema_defined) \
    .option("header",True) \
    .option("sep",";") \
    .load("/FileStore/tables/stream_read/")

df1 = df.groupBy("Shop").sum("Sale_count")    
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Streaming Data

# COMMAND ----------

df4 = df.writeStream.format("parquet") \
    .outputMode("append") \
    .option("path","/FileStore/tables/stream_write/") \
    .option("checkpointLocation","/FileStore/tables/stream_checkpoint/") \
    .start().awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## To Read Data File in Parquet Format

# COMMAND ----------

df5 = spark.read.parquet("/FileStore/tables/stream_write/part-0000-372418a9-e19a-4e75-9338-dd613a910413-c000.snappy.parquet/")
df5.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove the folder from DBFS File System

# COMMAND ----------

dbutils.fs.rm("/FileStore/tables/stream_read/", True)
dbutils.fs.rm("/FileStore/tables/stream_checkpoint/", True)
dbutils.fs.rm("/FileStore/tables/stream_write/", True)

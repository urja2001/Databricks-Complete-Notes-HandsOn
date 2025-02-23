# Databricks notebook source
# MAGIC %md
# MAGIC # Creating a DataFrame with a DDL Schema & StructType ()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DDL Schema
# MAGIC
# MAGIC https://vincent.doba.fr/posts/20211004_spark_data_description_language_for_defining_spark_schema/

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

# COMMAND ----------

# Initialize Spark Session

spark = SparkSession.builder.appName("DDL_Schema").getOrCreate()

# COMMAND ----------

# Define schema using DDL

ddl_schema = "id INT, name STRING, age INT, salary DOUBLE,country STRING"

# COMMAND ----------

# Create a DataFrame using schema

data = (1, "Alice", 30, 5000.0,"US"), (2, "Bob", 28, 6000.5,"IN"),(3, "John", 29, 7000.5,"UK")

# COMMAND ----------

# Create DataFrame

df = spark.createDataFrame(data, schema=ddl_schema)

# COMMAND ----------


# Correct way to view the data in DataFrame

df.display()

# To inspect the schema (DDL) of the DataFrame

df.printSchema()

# COMMAND ----------

# Create a DataFrame from a list of tuples

data1 = [(1, "Alice", 29), (2, "Bob", 35), (3, "Charlie", 30)]
columns = ["id", "name", "age"]

# COMMAND ----------

# Create DataFrame

df1 = spark.createDataFrame(data1, columns)

# COMMAND ----------

# Correct way to view the data in DataFrame

df1.display()

# To inspect the schema (StructType) of the DataFrame

print(df1.schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## StructType ()
# MAGIC 1. StructType(fields): Represents values with the structure described by a sequence of StructFields (fields).
# MAGIC
# MAGIC 2. StructField(name, dataType, nullable): Represents a field in a StructType. The name of a field is indicated by name. The data type of a field is indicated by dataType. nullable is used to indicate if values of these fields can have null values.
# MAGIC
# MAGIC Follow : https://spark.apache.org/docs/3.5.3/sql-ref-datatypes.html
# MAGIC
# MAGIC ![DataTypes image](files/tables/DataTypes.png)

# COMMAND ----------

from pyspark.sql.types import _parse_datatype_string

# COMMAND ----------

# Convert DDL to StructType

df2 = _parse_datatype_string(ddl_schema)

# COMMAND ----------

print(df2)

# COMMAND ----------

data = (1, "Alice", 30, 5000.0,"US"), (2, "Bob", 28, 6000.5,"IN"),(3, "John", 29, 7000.5,"UK")

# COMMAND ----------

# Create DataFrame

df3 = spark.createDataFrame(data, ddl_schema)

# COMMAND ----------

# Correct way to view the data in DataFrame

df3.display()

# To inspect the schema (StructType) of the DataFrame

print(df3.schema)

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

ddlschema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

# COMMAND ----------

data1 = [
    (1, "Alice"),
    (2, "Bob"),
    (3, "Charlie")
]


# COMMAND ----------


# Create DataFrame

df4 = spark.createDataFrame(data1, ddlschema)

# COMMAND ----------

# Correct way to view the data in DataFrame

df4.show()

# To inspect the schema (StructType) of the DataFrame

print(df4.schema)

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading a file using DDL Schema & StructType ()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DDL Schema

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

# COMMAND ----------

# Initialize SparkSession (only needed if running locally)

spark = SparkSession.builder.appName("ReadCSV").getOrCreate()

# COMMAND ----------

# header - The first row of file contains the column names. By Default header = False
# schema() - Pre-defined schema when reading your data (CSV, JSON, etc.)

rdd = (spark.read.format('csv') \
       .schema(my_ddl_schema)  \
       .load("/FileStore/tables/BigMart_Sales.csv"))    

# COMMAND ----------

display(rdd)

rdd.printSchema()

# COMMAND ----------

# Define the schema in DDL format

ddl_schema = """
    Identifier STRING,
    Weight DOUBLE,
    Fat_Content STRING,
    Visibility DOUBLE,
    Type STRING,
    MRP DOUBLE,
    Outlet_Identifier STRING,
    Outlet_Establishment_Year STRING,
    Outlet_Size STRING,
    Outlet_Location_Type STRING,
    Outlet_Type STRING,
    Item_Outlet_Sales DOUBLE
"""

# COMMAND ----------

# Read the CSV file with the DDL schema

rdd = spark.read.format("csv") \
        .schema(ddl_schema) \
        .option("header", "True") \
        .load("/FileStore/tables/BigMart_Sales.csv")

# COMMAND ----------

# Show the DataFrame

rdd.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## StructType ()

# COMMAND ----------

# header - The first row of file contains the column names. By Default header = False
# inferSchema - To determine the data type of each column in file. By Default inferSchema = False

rdd1 = spark.read.format("csv") \
    .option("header", "True") \
    .option("inferSchema", "True") \
    .load("/FileStore/tables/BigMart_Sales.csv")

# COMMAND ----------

display(rdd1)

rdd1.printSchema()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

my_ddl_schema = StructType([
    StructField("Identifier", StringType(), True),
    StructField("Weight", StringType(), True),  # Or "Item_Weight" if that's the real name
    StructField("Fat_Content", StringType(), True),
    StructField("Visibility", DoubleType(), True),
    StructField("Type", StringType(), True),
    StructField("MRP", DoubleType(), True),
    StructField("Outlet_Identifier", StringType(), True),
    StructField("Outlet_Establishment_Year", StringType(), True),
    StructField("Outlet_Size", StringType(), True),
    StructField("Outlet_Location_Type", StringType(), True),
    StructField("Outlet_Type", StringType(), True),
    StructField("Item_Outlet_Sales", DoubleType(), True)
])

# COMMAND ----------

# header - The first row of file contains the column names. By Default header = False
# schema() - Pre-defined schema when reading your data (CSV, JSON, etc.)

rdd2 = (spark.read.format('csv') \
       .schema(my_ddl_schema)  \
       .load("/FileStore/tables/BigMart_Sales.csv"))    

# COMMAND ----------

rdd2.display()

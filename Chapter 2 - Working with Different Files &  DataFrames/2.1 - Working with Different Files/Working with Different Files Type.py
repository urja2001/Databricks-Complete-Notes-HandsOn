# Databricks notebook source
# MAGIC %md
# MAGIC # To Check the location of Files 

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /FileStore/tables/Wordcount.txt
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **1. dbutils.fs.ls(path) - Lists the contents of a directory. Returns a list of file objects, each with information about the file (name, path, size, etc.).**

# COMMAND ----------

dbutils.fs.ls("/FileStore/")


# COMMAND ----------

dbutils.fs.ls("/FileStore/tables")

# COMMAND ----------

# MAGIC %md
# MAGIC **2. dbutils.fs.put(path, content, overwrite=False) - Writes content to a file. You provide the path, the content (as a string), and whether to overwrite an existing file.**

# COMMAND ----------

dbutils.fs.put("/FileStore/tables/demo.txt","""
The fluffy cat sat on the mat. The fluffy cat purred softly.  The cat liked to nap. The cat liked to play. The cat was very fluffy.The big dog barked loudly. The big dog ran fast. The dog chased the ball. The dog loved to play fetch. The dog was very big.The small bird sang sweetly. The small bird flew high. The bird built a nest. The bird laid eggs. The bird was very small.The green tree swayed gently. The green tree provided shade. The tree grew tall. The tree had many leaves. The tree was very green.The red car zoomed quickly. The red car was very fast. The car had a loud engine. The car was shiny. The car was very red.The blue sky was clear and bright. The blue sky had no clouds. The sky was a beautiful blue. The sky was vast. The sky was very blue.The yellow sun shone warmly. The yellow sun gave light. The sun was very bright. The sun was hot. The sun was very yellow.The purple flower bloomed beautifully. The purple flower smelled sweet. The flower was delicate. The flower was pretty. The flower was very purple.The orange fruit was delicious. The orange fruit was juicy. The fruit was sweet. The fruit was healthy. The fruit was very orange.The brown bear walked slowly. The brown bear looked for food. The bear was big and strong. The bear was furry. The bear was very brown.The white snow fell softly. The white snow covered the ground. The snow was cold. The snow was pretty. The snow was very white.The black night was dark and quiet. The black night had many stars. The night was peaceful. The night was mysterious. The night was very black.The silver moon shone brightly. The silver moon gave light at night. The moon was round. The moon was beautiful. The moon was very silver.The golden stars twinkled brightly. The golden stars were far away. The stars were beautiful. The stars were magical. The stars were very golden.The happy child laughed loudly. The happy child played all day. The child was joyful. The child was energetic. The child was very happy.The big dog barked loudly. The big dog ran fast. The dog chased the ball. The dog loved to play fetch. The dog was very big.The small bird sang sweetly. The small bird flew high. The bird built a nest. The bird laid eggs. The bird was very small.The green tree swayed gently. The green tree provided shade. The tree grew tall. The tree had many leaves. The tree was very green.The red car zoomed quickly. The red car was very fast. The car had a loud engine. The car was shiny. The car was very red.The blue sky was clear and bright. The blue sky had no clouds. The sky was a beautiful blue. The sky was vast. The sky was very blue.The yellow sun shone warmly. The yellow sun gave light. The sun was very bright. The sun was hot. The sun was very yellow.The purple flower bloomed beautifully. The purple flower smelled sweet. The flower was delicate. The flower was pretty. The flower was very purple.The orange fruit was delicious. The orange fruit was juicy. The fruit was sweet. The fruit was healthy. The fruit was very orange.The brown bear walked slowly. The brown bear looked for food. The bear was big and strong. The bear was furry. The bear was very brown.The white snow fell softly. The white snow covered the ground. The snow was cold. The snow was pretty. The snow was very white.The black night was dark and quiet. The black night had many stars. The night was peaceful. The night was mysterious. The night was very black.The silver moon shone brightly. The silver moon gave light at night. The moon was round. The moon was beautiful. The moon was very silver.The golden stars twinkled brightly. The golden stars were far away. The stars were beautiful. The stars were magical. The stars were very golden.The happy child laughed loudly. The happy child played all day. The child was joyful. The child was energetic. The child was very happy.
""")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables")

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/demo.txt"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Files Different Types of Files

# COMMAND ----------

help(sc.textFile)

# COMMAND ----------

rdd = sc.textFile("/FileStore/tables/Wordcount.txt")

# COMMAND ----------

rdd.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC **Types of Format**
# MAGIC
# MAGIC **1. "csv": Comma-separated values. Use this for files where data is organized in rows and columns, separated by commas. You'll often use the header and inferSchema options along with "csv". spark.read.csv() or spark.read.format("csv") .**
# MAGIC
# MAGIC **2. "json": JavaScript Object Notation. Use this for files containing JSON data. You might use the multiline option if you have complex JSON objects that span multiple lines. spark.read.json() or spark.read.format("json") .**
# MAGIC
# MAGIC **3. "parquet": A columnar storage format that is highly efficient for analytical queries. Parquet is often the preferred format for storing large datasets in Spark. spark.read.parquet() or spark.read.format("parquet") .**
# MAGIC
# MAGIC **4. "orc": Optimized Row Columnar, another efficient columnar storage format. Similar to Parquet in many ways. spark.read.orc() or spark.read.format("orc") .**
# MAGIC
# MAGIC **5. "avro": A binary serialization format often used with Hadoop. spark.read.avro() or spark.read.format("avro") .**
# MAGIC
# MAGIC **6. "text": Plain text files. Each line in the file becomes a row in the DataFrame. spark.read.text() or spark.read.format("text") .**
# MAGIC
# MAGIC **7. "delta": Delta Lake, a storage layer that brings reliability to data lakes. spark.read.delta() or  spark.read.format("delta") .**
# MAGIC
# MAGIC **8. "libsvm": A format used for machine learning datasets.**

# COMMAND ----------

help(spark.read)

# COMMAND ----------

help(spark.read.text)

# COMMAND ----------

rdd1 = spark.read.text("/FileStore/tables/demo.txt")

# COMMAND ----------

rdd1.collect()

# COMMAND ----------

rdd2 = spark.read.format("text").load("/FileStore/tables/demo.txt")


# COMMAND ----------

rdd2.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC **2. CSV**

# COMMAND ----------

help(spark.read.csv)

# COMMAND ----------

rdd3 = spark.read.csv("/FileStore/tables/BigMart_Sales.csv")

# COMMAND ----------

rdd3.show()

# COMMAND ----------

rdd3.display()

# COMMAND ----------

rdd4 = spark.read.csv("/FileStore/tables/BigMart_Sales.csv",header="True",inferSchema="True")

# COMMAND ----------

rdd4.show()

# COMMAND ----------

display(rdd4)

# COMMAND ----------

rdd4.printSchema()

# COMMAND ----------

rdd5 = spark.read.format("csv").options(header="True",inferSchema="True").load("/FileStore/tables/BigMart_Sales.csv")

# COMMAND ----------

rdd5.display()

# COMMAND ----------

# header - The first row of file contains the column names. By Default header = False
# inferSchema - To determine the data type of each column in file. By Default inferSchema = False

rdd6 = spark.read.format("csv") \
    .option("header", "True") \
    .option("inferSchema", "True") \
    .load("/FileStore/tables/BigMart_Sales.csv")

# COMMAND ----------

rdd6.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **3. JSON**

# COMMAND ----------

dbutils.fs.put("/FileStore/tables/test1.json","""
{
  "person1": {
    "name": "John Doe",
    "age": 30,
    "city": "New York"
  },
  "person2": {
    "name": "Jane Smith",
    "age": 25,
    "city": "London"
  },
  "person3": {
    "name": "David Lee",
    "age": 40,
    "city": "Paris"
  },
  "person4": {
    "name": "Sarah Jones",
    "age": 35,
    "city": "Tokyo"
  },
  "person5": {
    "name": "Michael Brown",
    "age": 28,
    "city": "Sydney"
  },
  "person6": {
    "name": "Emily Davis",
    "age": 32,
    "city": "Berlin"
  },
  "person7": {
    "name": "Robert Wilson",
    "age": 45,
    "city": "Madrid"
  },
  "person8": {
    "name": "Ashley Garcia",
    "age": 29,
    "city": "Rome"
  },
  "person9": {
    "name": "William Rodriguez",
    "age": 38,
    "city": "Toronto"
  },
  "person10": {
    "name": "Elizabeth Martinez",
    "age": 31,
    "city": "Sao Paulo"
  }
}""")

# COMMAND ----------

help(spark.read.json)

# COMMAND ----------

rdd7 = spark.read.json("/FileStore/tables/test1.json", multiLine=True)  # By Default multiLine=False


# COMMAND ----------

rdd6.collect()

# COMMAND ----------

rdd7.show()

# COMMAND ----------

rdd7.display()

# COMMAND ----------

# multiline - To read the entire multi-line block as one JSON object. By Default multiline = False   

rdd8 = spark.read.format("json") \
    .option("multiline", "true") \
    .load("/FileStore/tables/test1.json")                    

# COMMAND ----------

rdd8.collect()

# COMMAND ----------

rdd8.display()

# COMMAND ----------

rdd8.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Write and Save Formatted Files 

# COMMAND ----------

rdd8.write.format("json").save("/FileStore/tables/test")


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/test

# COMMAND ----------

rdd8.write.format("json").option("path","/FileStore/tables/test").mode("append").save()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/test

# COMMAND ----------

rdd8.write.format("json").option("path","/FileStore/tables/test").mode("overwrite").save()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/test

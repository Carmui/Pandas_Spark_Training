# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, min, max, avg, mean, count

spark = SparkSession.builder.appName("D8 WORD COUNT").getOrCreate()

# COMMAND ----------

df = spark.read.options(inferSchema = "True").csv('/FileStore/tables/WordData.txt')

# COMMAND ----------

df = df.select(col("_c0").alias("Names"))
df.show()

# COMMAND ----------

# Word count of the txt file
df.groupBy("Names").agg(count('*').alias("Count_of_names")).show()

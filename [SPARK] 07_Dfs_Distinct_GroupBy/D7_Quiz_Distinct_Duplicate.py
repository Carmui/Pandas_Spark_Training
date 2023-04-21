# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder.appName("Quiz Dyplicates").getOrCreate()

# COMMAND ----------

df = spark.read.options(inferSchema = "True", header = "True").csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df2 = df.select(['age', 'gender', 'course'])
df2.distinct().show()

# COMMAND ----------

df3 = df.dropDuplicates(['age', 'gender', 'course'])
df3.show()

# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, sum, avg, max, min, mean, count

spark = SparkSession.builder.appName("D8 Quiz GroupBy").getOrCreate()

# COMMAND ----------

df = spark.read.options(inferSchema = "True", header = "True").csv('/FileStore/tables/StudentData.csv')

# COMMAND ----------

df.show()

# COMMAND ----------

# Display the total numbers of students enrolled in each course
df.groupBy("course").agg(count("*").alias("total_enrollment")).show()

# COMMAND ----------

# Display the total number of male nad female students enrolled in each course
df.groupBy("course", "gender").agg(count("*").alias("total_enrollment")).show()

# COMMAND ----------

# Display the total marks achieved by each gender in each course
df.groupBy("course", "gender").agg(sum("marks").alias("total_marks")).show()

# COMMAND ----------

# Display the minimum, max, avg marks achieved in each course by each age group
df.groupBy("course", "age").agg(min("marks").alias("min_mark"), max("marks").alias("max_mark"), avg("marks").alias("avg_mark")).show()

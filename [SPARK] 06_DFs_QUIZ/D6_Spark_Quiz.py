# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Day 6 Quiz").getOrCreate()

# COMMAND ----------

df = spark.read.options(header = 'True', inferSchema = 'True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

from pyspark.sql.functions import col, lit

df = df.withColumn("Total_marks", lit(120))
df.show()

# COMMAND ----------

df = df.withColumn("average", col("marks")/col("Total_marks")*100)
df.show(5)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType

df = df.withColumn("average", df.average.cast(DecimalType(18, 2)))
df.show()

# COMMAND ----------

# Filter out all students who have achieved more than 80% marks in OOP course and save to new DF
oop_df = df.filter((col("course") == 'OOP') & (df.average > 80))
oop_df.show()

# COMMAND ----------

# Filter out all students who have achieved more than 60% marks in Cloud course and save to new DF
cloud_df = df.filter((col("course") == 'Cloud') & (df.average > 60))
cloud_df.show()

# COMMAND ----------

oop_df.select("name", "marks").show()

# COMMAND ----------

cloud_df.select("name", "marks").show()

# COMMAND ----------

# print the names and marks of all the students from the above DFS
df.select("name", "marks").filter(df.name.isin([cloud_df.name, oop_df.name])).show()



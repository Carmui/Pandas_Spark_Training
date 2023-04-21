# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col


spark = SparkSession.builder.appName("Quiz 7 orderBy").getOrCreate()

# COMMAND ----------

df = spark.read.options(inferSchema = "True", header = "True").csv('/FileStore/tables/OfficeData.csv')
df.show(5)

# COMMAND ----------

# Create a DF sorted on bonus in ascending order and show it
bonus_sorted = df.sort(df.bonus.asc())
bonus_sorted.show(5)

# COMMAND ----------

# Create a DF sorted on age and salary in descending and ascending order respectively and show it
bonus_sorted_2 = df.orderBy(df.age.desc(), df.salary.asc())
bonus_sorted_2.show(5)

# COMMAND ----------

# Create a DF sorted on age, bonus, salary in desc, desc and asc order and show it
bonus_sorted_3 = df.orderBy(df.age.desc(), df.bonus.desc(), df.salary.asc())
bonus_sorted_3.show(5)

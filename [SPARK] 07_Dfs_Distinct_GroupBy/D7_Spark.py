# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder.appName("Distinct, count, duplicate").getOrCreate()

# COMMAND ----------

df = spark.read.options(header = 'True', inferSchema = 'True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df.filter(df.course == "DB").count()

# COMMAND ----------

df.distinct().count()

# COMMAND ----------

df.select(col("gender")).distinct().count()

# COMMAND ----------

df.select(col("gender"), col("age")).distinct().show()

# COMMAND ----------

df.dropDuplicates(["gender"]).show()

# COMMAND ----------

df.show()

# COMMAND ----------

df.dropDuplicates(["gender", "course"]).show()

# COMMAND ----------

#Sort, orderby
df.sort("marks", "age").show()

# COMMAND ----------

df.sort(df.marks.asc(), df.age.desc()).show()

# COMMAND ----------

df.orderBy(df.marks.asc(), df.age.desc()).show()

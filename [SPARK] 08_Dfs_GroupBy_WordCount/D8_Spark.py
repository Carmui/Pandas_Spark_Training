# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col

spark = SparkSession.builder.appName("D8 app").getOrCreate()

# COMMAND ----------

df = spark.read.options(inferSchema = "True", header = "True").csv('/FileStore/tables/StudentData.csv')
df.show(5)

# COMMAND ----------

df.groupBy("gender").sum("marks").show()

# COMMAND ----------

df.groupBy("gender").count().show()
df.groupBy("course").count().show()

# COMMAND ----------

df.groupBy("course").max("marks").show()

# COMMAND ----------

df.groupBy("course").avg("marks").show()

# COMMAND ----------

# Spark DF Group By - Multiple columns
df.groupBy("course", "gender").count().show()

# COMMAND ----------

from pyspark.sql.functions import sum, avg, max, min, mean, count

df.groupBy("course").agg(count("*"), sum("marks"), min("marks")).show()

# COMMAND ----------

df.groupBy("course").agg(count("*").alias("total_enrollments"), sum("marks").alias("total_marks"), min("marks").alias("min_marks")).show()

# COMMAND ----------

# Filter group by
df_x = df.filter(df.gender == "Male")
df_y = df_x.groupBy("course", "gender").agg(count('*').alias("total_enrollments"))

# COMMAND ----------

df.groupBy("gender").agg(count('*').alias("total_enrollments")).filter(df.gender == "Male").show()

# COMMAND ----------

df_y.filter(df_y.total_enrollments > 55).show()

# COMMAND ----------



# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf

spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header = 'True', inferSchema = 'True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

type(df)

# COMMAND ----------

rdd = df.rdd

# COMMAND ----------

rdd.collect()

# COMMAND ----------

rdd.filter(lambda x: x[1] == "Male").collect()

# COMMAND ----------

df.createOrReplaceTempView("Student")

# COMMAND ----------

spark.sql("select course, gender, sum(marks) from Student where group by course, gender").show()

# df.select("course").show()

# COMMAND ----------

# Write DF
df.write.options(header = 'True').csv('/FileStore/tables/StudentData/output')



# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

# overwrite
# append
# ignore mode
# error

df.write.mode("overwrite").options(header = 'True').csv('/FileStore/tables/StudentData/output')
df.write.mode("append").options(header = 'True').csv('/FileStore/tables/StudentData/output')
df.write.mode("ignore").options(header = 'True').csv('/FileStore/tables/StudentData/output')
df.write.mode("overwrite").options(header = 'True').csv('/FileStore/tables/StudentData/output')

# COMMAND ----------



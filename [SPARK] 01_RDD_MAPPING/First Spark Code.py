# Databricks notebook source
from pyspark import SparkConf, SparkContext


# COMMAND ----------

conf = SparkConf().setAppName("Read File")

# COMMAND ----------

sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

text = sc.textFile('data.txt')

# COMMAND ----------

print(text.collect())

# COMMAND ----------



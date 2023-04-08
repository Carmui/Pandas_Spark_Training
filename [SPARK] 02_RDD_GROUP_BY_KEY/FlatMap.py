# Databricks notebook source
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("FlatMAP")

sc = SparkContext.getOrCreate(conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/data.txt')
rdd.collect()

# COMMAND ----------

maprdd = rdd.map(lambda x: x.split())
maprdd.collect()

# COMMAND ----------

Flatmaprdd = rdd.flatMap(lambda x: x.split())
Flatmaprdd.collect()

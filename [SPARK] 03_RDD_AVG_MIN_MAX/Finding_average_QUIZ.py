# Databricks notebook source
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Finding average")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/average_quiz_sample.csv')
rdd.collect()

# COMMAND ----------

# Write a code to calculate the average score in each month
rdd_1 = rdd.map(lambda x: (x.split(',')[0], (float(x.split(',')[2]), 1.0)))
reduced_rdd = rdd_1.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
avg_rdd = reduced_rdd.map(lambda x: (x[0], x[1][0]/x[1][1]))

# COMMAND ----------

avg_rdd.collect()

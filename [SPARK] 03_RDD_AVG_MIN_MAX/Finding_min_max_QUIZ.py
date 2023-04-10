# Databricks notebook source
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Finding min & max")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/average_quiz_sample.csv')
rdd.collect()

# COMMAND ----------

# Write a code to calculate the Minimum and Maximum rating given by each city

#minimum
first_rdd = rdd.map(lambda x: x.split(',')).map(lambda x: (x[1], float(x[2])))
first_rdd.reduceByKey(lambda x,y: x if x < y else y).collect()

# COMMAND ----------

#maximum
first_rdd = rdd.map(lambda x: x.split(',')).map(lambda x: (x[1], float(x[2])))
first_rdd.reduceByKey(lambda x,y: x if x > y else y).collect()

# Databricks notebook source
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Word count")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/word_count_quiz.txt')
rdd.collect()

# COMMAND ----------

#Solution 1 - Number of letters per word 
flat_mapped = rdd.flatMap(lambda x: x.split()).map(lambda x: (x, len(x))).distinct()
flat_mapped.collect()

# COMMAND ----------

#Solution 2 - Number of words in the txt file
flat_mapped_2 = rdd.flatMap(lambda x: x.split()).map(lambda x: (x, 1))

final_result = flat_mapped_2.reduceByKey(lambda x,y: x + y)
final_result.collect()

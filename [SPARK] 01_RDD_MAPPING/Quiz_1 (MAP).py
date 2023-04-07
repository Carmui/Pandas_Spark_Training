# Databricks notebook source
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Read file")
sc = SparkContext.getOrCreate(conf)

# COMMAND ----------

data_spark = sc.textFile('/FileStore/tables/data_quiz_1.txt')
data_spark.collect()

# COMMAND ----------

# Solution 1
new_rdd = data_spark.map(lambda x: [len(i) for i in x.split()])
new_rdd.collect()

# COMMAND ----------

# Solution 2
def map_function(x):
    splitted = x.split()
    new_list = []
    for split in splitted:
        new_list.append(len(split))
    
    return new_list

new_rdd_2 = data_spark.map(map_function)    
new_rdd_2.collect()

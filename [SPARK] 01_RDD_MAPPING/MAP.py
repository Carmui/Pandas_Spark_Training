# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Read File")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/data.txt')

# COMMAND ----------

new_rdd = rdd.map(lambda x: x.split())

# COMMAND ----------

new_rdd_2 = rdd.map(lambda x: x + "Matt")

# COMMAND ----------

new_rdd_2.collect()

# COMMAND ----------

def foo(x):  
    return x.split()

rdd_function = rdd.map(foo)
rdd_function.collect()

# COMMAND ----------

def foo(x):   
    l = x.split()
    l2 = []
    for s in l:
        l2.append(int(s) + 2)
        
    return l2

rdd_function_2 = rdd.map(foo)
rdd_function_2.collect()

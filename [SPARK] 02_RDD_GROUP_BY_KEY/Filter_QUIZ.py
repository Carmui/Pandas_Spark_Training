# Databricks notebook source
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Filter QUIZ")
sc = SparkContext.getOrCreate(conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/data_filter.txt')
rdd.collect()

# COMMAND ----------

# WRITE A FILTER THAT WILL REMOVE ALL THE WORD THAT START WITH 'a' or 'c'
flat_rdd = rdd.flatMap(lambda x: x.split())
flat_rdd.collect()

# COMMAND ----------

# Solution 1
def remover(x) -> bool:
    if x[0] != 'a' and x[0] != 'c':
        return True
    else:
        return False

function_flat_rdd = flat_rdd.filter(remover)
function_flat_rdd.collect()

# COMMAND ----------

# Solution 1.2 
def remover_func(x) -> bool:
    if x.startswith('a') or x.startswith('c'):
        return False
    else:
        return True

function_flat_rdd_2 = flat_rdd.filter(remover_func)
function_flat_rdd_2.collect()

# COMMAND ----------

# Solution 2 
lambda_flat_rdd = flat_rdd.filter(lambda x: x[0] != 'a' and x[0] != 'c')
lambda_flat_rdd.collect()

# COMMAND ----------



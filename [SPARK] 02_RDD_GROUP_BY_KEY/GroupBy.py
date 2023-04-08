# Databricks notebook source
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Distinct")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/data.txt')
rdd.collect()

# COMMAND ----------

rdd2 = rdd.flatMap(lambda x: x.split())
rdd3 = rdd2.distinct()
rdd3.collect()

# COMMAND ----------

rdd.flatMap(lambda x: x.split()).distinct().collect()

# COMMAND ----------

# groupByKey
# mapValues(list)
rdd_group = sc.textFile('/FileStore/tables/data_filter.txt')
rdd_group.collect()

# COMMAND ----------

rdd_group.map(lambda x: (x, len(x.split()))).collect()

# COMMAND ----------

rdd_new = rdd_group.flatMap(lambda x: x.split()).map(lambda x: (x, len(x)))

# COMMAND ----------

rdd_new.groupByKey().mapValues(list).collect()

# COMMAND ----------

#reduceByKey - it is going to reduce number of list elements from groupByKey by the function passed
rdd_reduce = rdd.flatMap(lambda x: x.split())

# COMMAND ----------

rdd_reduce2 = rdd_reduce.map(lambda x: (x,1))

# COMMAND ----------

rdd_reduce2.reduceByKey(lambda x, y: x+y).collect()

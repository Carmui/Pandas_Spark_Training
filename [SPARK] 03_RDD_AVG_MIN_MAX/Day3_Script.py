# Databricks notebook source
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Day 3 script")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/data_filter.txt')
rdd.collect()

# COMMAND ----------

rdd.flatMap(lambda x: x.split()).count()

# COMMAND ----------

# Count by Value
rdd.countByValue()

# COMMAND ----------

rdd.flatMap(lambda x: x.split()).countByValue()

# COMMAND ----------

# saveAsTextFile
rdd.saveAsTextFile('/FileStore/tables/output/data_filter_saved.txt')

# COMMAND ----------

rdd.getNumPartitions()

# COMMAND ----------

rdd2 = rdd.flatMap(lambda x: x.split())
rdd3 = rdd2.map(lambda x: (x,1))

# COMMAND ----------

rdd3.collect()

# COMMAND ----------

rdd3.getNumPartitions()

# COMMAND ----------

# Coalesce and repartition
rdd_rep = sc.textFile('/FileStore/tables/data_filter.txt')
rdd_rep = rdd_rep.repartition(5)
rdd2 = rdd_rep.flatMap(lambda x: x.split())
rdd3 = rdd2.map(lambda x: (x,1))

# COMMAND ----------

#rdd3.saveAsTextFile('/FileStore/tables/output/5_partition_output')

# COMMAND ----------



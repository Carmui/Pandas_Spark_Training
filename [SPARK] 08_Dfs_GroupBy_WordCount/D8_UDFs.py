# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, udf
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("D8 Quiz GroupBy").getOrCreate()

# COMMAND ----------

df = spark.read.options(inferSchema = "True", header = "True").csv('/FileStore/tables/OfficeData.csv')

# COMMAND ----------

df.show()

# COMMAND ----------

def get_Total_Salary(salary, bonus):
    return salary + bonus
    
totalSalaryUDF = udf(lambda x,y :get_Total_Salary(x, y), IntegerType())    
    
df.withColumn("total_salary", totalSalaryUDF(df.salary, df.bonus)).show()

# COMMAND ----------



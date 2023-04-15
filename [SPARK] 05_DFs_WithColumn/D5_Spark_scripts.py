# Databricks notebook source
#SparkSession - DF, SparkContext - RDD
from pyspark import SparkContext, SparkConf 
from pyspark.sql import SparkSession

# Only one spark session at the time
# If there is only create -> spark my return exception -> so better to use getOrCreate
spark = SparkSession.builder.appName("First DF app").getOrCreate()


# COMMAND ----------

df = spark.read.options(header= 'True', inferSchema= 'True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
                    StructField("age", IntegerType(), True),
                    StructField("gender", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("course", StringType(), True),
                    StructField("roll", StringType(), True),
                    StructField("marks", IntegerType(), True),
                    StructField("email", StringType(), True)
])



# COMMAND ----------

df = spark.read.options(header= 'True').schema(schema).csv('/FileStore/tables/StudentData.csv')
df.show()
df.printSchema()

# COMMAND ----------

conf = SparkConf().setAppName("RDD APP") 
sc = SparkContext.getOrCreate(conf=conf)

rdd = sc.textFile('/FileStore/tables/StudentData.csv')
headers = rdd.first()
rdd = rdd.filter(lambda x: x != headers).map(lambda x: x.split(','))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
                    StructField("age", IntegerType(), True),
                    StructField("gender", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("course", StringType(), True),
                    StructField("roll", StringType(), True),
                    StructField("marks", IntegerType(), True),
                    StructField("email", StringType(), True)
])



# COMMAND ----------

columns = headers.split(',')
dfRdd = spark.createDataFrame(rdd, schema=schema)
# dfRdd.show()
dfRdd.printSchema()

# COMMAND ----------

# Rectify error

conf = SparkConf().setAppName("RDD APP") 
sc = SparkContext.getOrCreate(conf=conf)

rdd = sc.textFile('/FileStore/tables/StudentData.csv')
headers = rdd.first()
rdd = rdd.filter(lambda x: x != headers).map(lambda x: x.split(','))
rdd = rdd.map(lambda x: [int(x[0]), x[1], x[2], x[3], x[4], int(x[5]), x[6]])
rdd.collect()

# COMMAND ----------

columns = headers.split(',')
dfRdd = spark.createDataFrame(rdd, schema=schema)
dfRdd.show()
# dfRdd.printSchema()

# COMMAND ----------

# Select DataFrameColumns
df = spark.read.options(header= 'True', inferSchema= 'True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df.select("name", "gender").show()

# COMMAND ----------

df.select(df.name, df.gender).show(5)

# COMMAND ----------

from pyspark.sql.functions import col 
df.select(col("name"), col("roll")).show(5)

# COMMAND ----------

df.select('*').show(5)

# COMMAND ----------

df.select(df.columns[2:6]).show()

# COMMAND ----------

df.select(*df.columns[0:3], 'marks', col('email'), df.roll, "course").show()

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumn("roll", col("roll").cast("String"))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show()

# COMMAND ----------

df = df.withColumn("marks", col("marks") + 10)
df.show()

# COMMAND ----------

df = df.withColumn("marks", col("marks") - 10)
df.show()

# COMMAND ----------

df = df.withColumn("aggregated marks", col("marks") - 10)
df.show()

# COMMAND ----------

from pyspark.sql.functions import lit

df = df.withColumn("Country", lit("USA"))
df.show()

# COMMAND ----------

# Select DataFrameColumns
df = spark.read.options(header= 'True', inferSchema= 'True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

# Rename columns

df = df.withColumnRenamed("gender", "sex")
df.show()

# COMMAND ----------

# Filtering Data from DF
df.filter(df.course == "DB").show()

# COMMAND ----------

df.filter(col("course") == "DB").show()

# COMMAND ----------

df.filter((df.course == "DB") & (df.marks > 50)).show()

# COMMAND ----------

df.filter((df.course == "DB") | (df.course == "Cloud")).show()

# COMMAND ----------

courses = ["DB", "Cloud", "OOP", "DSA"]

df.filter( df.course.isin(courses)).show()

# COMMAND ----------

df.filter(df.course.startswith("D")).show()

# COMMAND ----------

df.filter(df.name.endswith("se")).show()

# COMMAND ----------

df.filter(df.name.contains("se")).show()

# COMMAND ----------

df.filter(df.name.like("%se%")).show()

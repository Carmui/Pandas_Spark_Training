# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, udf, avg, max, min, mean, count

spark = SparkSession.builder.appName("Dat 10 quiz").getOrCreate()

# COMMAND ----------

df = spark.read.options(header = 'True', inferSchema = 'True').csv('/FileStore/tables/OfficeDataProject__1_.csv')

# COMMAND ----------

df.show(5)

# COMMAND ----------

# 1. Print the total number of employees in the company

# Count the rows
df.count()

# Count the distinct employee_id
df.select('employee_id').distinct().count()

# COMMAND ----------

# 2. Print the total number of departments in the company

# Number of departments
df.select('department').distinct().count()

# COMMAND ----------

# 3. Print the department names of the company
df.select('department').distinct().show()

# COMMAND ----------

# 4. Print the total number of employees in each department

# Group by number of employees in each deparment
df.groupBy('department').count().show()

# COMMAND ----------

# 5. Print the total number of employees in each state

df.groupBy('state').count().show()

# COMMAND ----------

# 6. Print the total number of employees in each state in each department

df.groupBy(['state', 'department']).count().show()

# COMMAND ----------

# 7. Print the minimum and maximum salaries in each department and sort salaries in ascending order

df.groupBy('department').agg(min('salary').alias("minimum_sal"), max('salary').alias("maximum_sal")).orderBy(col('maximum_sal').asc()).show()

# COMMAND ----------

# 8. Print the names of employees working in NY state under Finance deparment whose bonuses are greater than the average bonuses in employees in NY state
avg_df = df.filter(df.state == "NY").groupBy('state').agg(avg('bonus').alias("avg_bon")).select('avg_bon').collect()[0]['avg_bon']

df.filter((df.state == "NY") & (df.department == "Finance") & (df.bonus > avg_df)).select('employee_name').show()

# COMMAND ----------

from pyspark.sql.types import IntegerType

# 9. Raise the salaries $500 of all employees whose age is greater than 45 
def increment_salary(age, current_salary):
    if age > 45:
        new_salary = current_salary + 500
    else:
        new_salary = current_salary
    
    return new_salary


incUDF = udf(lambda x,y: increment_salary(x, y), IntegerType())
df.withColumn("increment_salary", incUDF(df.age, df.salary)).show()

# COMMAND ----------

# 10. Create DF of all those employees whose age is greater than 45 and save them in a file

df.filter(df.age > 45).write.csv('/FileStore/tables/output_45')

# COMMAND ----------

df.show(5)

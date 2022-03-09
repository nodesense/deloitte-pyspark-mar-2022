# Databricks notebook source
orders = [
    # symbol, qty
    ('INFY', 200),
    ('TSLA', 50),
    ('EMR', 20),
    ('INFY', 100),
    ('TSLA', 25)
]
# spark session - entry point for data frame sql
# create a dataframe from hardcoded data
# schema is built using data inference, column is not given us
# scan the data, derive data types automatically
df = spark.createDataFrame(data=orders, schema=['symbol', 'qty'])

df.printSchema()
df.show() # print 20 records

# COMMAND ----------

# DF is an API,  no data inside, it has RDD, all data resides in RDD partitions
# any opeartions performed on dataframe are immutable
# DF has Rdd[Row]
# DF quries are optimized by catalyser. Catalizer internally generate JVM byte code for every DF, SQL queries we run
# on spark

print(df.rdd.count())
print(df.rdd.getNumPartitions())
df.rdd.collect()

# COMMAND ----------

df2 = df.select ("symbol","qty")\
         .where ( df.qty >= 100 )

df2.show()

# COMMAND ----------

df2 = df.sort("qty")
df2.show()

# COMMAND ----------

df2 = df.select("symbol").distinct()
df2.show()

# COMMAND ----------


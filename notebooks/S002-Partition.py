# Databricks notebook source
data = range(1, 20)
rdd = sc.parallelize(data)

print ("default min partition ", sc.defaultMinPartitions) # HDFS min paritions
# defaultParallelism how many parallel tasks can be run at any time
print ("default parallelism", sc.defaultParallelism) # used by parallelize method

# COMMAND ----------

# get number of partitions, LAZY Evalaution
rdd.getNumPartitions()

# COMMAND ----------

# collect 
# collect data from all paritions, then merge result into single list
rdd.collect()

# COMMAND ----------

# glom, action method
# collect data from all parition, but data returned as is partition data, , not merged into single list
rdd.glom().collect()

# COMMAND ----------

# load data into 2 paritions
rdd2 = sc.parallelize(data, 2)
rdd2.glom().collect()

# COMMAND ----------

rdd.glom().collect()

# COMMAND ----------

# take method collect data, first it read from parition 0 onwards with limit 
rdd.take(3)

# COMMAND ----------


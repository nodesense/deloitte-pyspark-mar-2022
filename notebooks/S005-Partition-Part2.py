# Databricks notebook source
# programmically increase/decrease paritions

data = range(1, 20)

rdd = sc.parallelize(data)

# COMMAND ----------

print(rdd.getNumPartitions())
rdd.glom().collect()

# COMMAND ----------

# how do we increase partitions to 12
# while increase partitions, spark take data  from several paritions, move them into differnt machine etc
# distribute the data
rdd2 = rdd.repartition(12)
print(rdd2.getNumPartitions())
rdd2.glom().collect()

# COMMAND ----------

# how to reduce the partitions
# we reduce 8 partitions into 2 partitions
rdd3 = rdd.coalesce(2)

print(rdd3.getNumPartitions())
rdd3.glom().collect()

# COMMAND ----------


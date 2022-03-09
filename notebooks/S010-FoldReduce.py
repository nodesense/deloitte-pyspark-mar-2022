# Databricks notebook source
""" copied from spark cluster Logs
acc 0 value 10 output 10
acc 10 value 20 output 30
acc 30 value 30 output 60
acc 60 value 40 output 100
acc 100 value 50 output 150
"""
def add(acc, value):
    result = acc + value
    print("acc", acc, "value", value, "output", result)
    return result
  

# COMMAND ----------

rdd = sc.parallelize([10,20,30,40,50], 1)


# COMMAND ----------


# fold is useful for aggregate
# fold has initial value to start with where as reduce will  take first value as reduce

# fold with aggregate with start value 0
# fold is action method
# fold works with each partition first, calculate add function on each partition
# + it will apply result of all paritions into again another folder
# return value of add is passed as input with next number in seq

# after processing data from partition 0, it got result 150
# then it will apply add function across partition result  acc 0 value 150
rdd.fold (0, add) # output shall be 150 = 10 + 20 + 30 + 40 + 50

# COMMAND ----------

# FoldByKey
# similar to fold, where as fold is applied on all the values in RDD in partition
# foldByKey is used against (Key,Value) paired rdd, key/value rdd
# fold work based on key

orders = [
    # symbol, qty
    ('INFY', 200),
    ('TSLA', 50),
    ('EMR', 20),
    ('INFY', 100),
    ('TSLA', 25)
]

def add(acc, value):
    output = acc + value
    print("acc", acc, "value", value, "output", output)
    return output

orderRdd = sc.parallelize(orders)
# fold by Key, return rdd
# When key appear first, it starts with 0, and value
# second appearance key, include previous output as input
orderRdd.foldByKey(0, add).collect()

# COMMAND ----------

# reduce
rdd = sc.parallelize([10,20,30,40,50], 1)
# reduce is similar like fold used for aggregation
# in reduce, the first is taken as seed value/initial value
# in fold, we pass initial value
# accumulator starts with 10 , NOT 0 for reduce
# Action
rdd.reduce(lambda acc, value: acc + value)

# COMMAND ----------

# reduceByKey
# used for aggregation
# similar to foldByKey, but the difference is foldByKey takes initial data as input
# reduceByKey uses first key value as initial value, example, for INFY, acc is 200
orders = [
    # symbol, qty
    ('INFY', 200),
    ('TSLA', 50),
    ('EMR', 20),
    ('INFY', 100),
    ('TSLA', 25)
]

orderRdd = sc.parallelize(orders)

rdd2 = orderRdd.reduceByKey(lambda acc, value: acc + value)
rdd2.collect()

# COMMAND ----------


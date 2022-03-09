# Databricks notebook source
numbersRdd = sc.parallelize(range(1, 10))
# try to split odd/even number as group
# n % 2 = the output shall be either 0 or 1, use this for grouping the numbers
# groupBy used to group by any data in your record
# groupByKey  where only key is used
# lambda either return 0 or 1, the items are grouped based that result
resultRdd = numbersRdd.groupBy(lambda n: n % 2)
# groupByRdds collect won't return list or tuple directly, we need to iterate and 
# get the output
results = resultRdd.collect()
# result is a list of tuple 
# result = [ (0, iterator), (1, iterator) ]
# 0 - even number group
# 1 - odd number group
for key, valueItr in results:
    print("Group ", key, valueItr)
    # iterator the result to get the actual data
    for value in valueItr:
        print ("\tVAlue ", value)

# COMMAND ----------

orders = [
    # symbol, qty
    ('INFY', 200),
    ('TSLA', 50),
    ('EMR', 20),
    ('INFY', 100),
    ('TSLA', 25)
]

orderRdd = sc.parallelize(orders)

resultRdd = orderRdd.groupBy (lambda orderTuple: orderTuple[0]) # return key as groupBy attribute
results = resultRdd.collect()


for key, valueItr in results:
    print("Group ", key, valueItr)
    # iterator the result to get the actual data
    for value in valueItr:
        print ("\tVAlue ", value)

# COMMAND ----------


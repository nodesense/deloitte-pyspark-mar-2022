# Databricks notebook source
stocks = [ # LIST
    # Tuple (symbol, open, low, high, close)
    ('MSFT', 50, 48, 51, 50.5),
     ('FB', 200, 198, 204, 202.3),
    ('AMD', 90, 88, 92, 91),
    ('TSLA', 100, 90, 90, 100)
]
rdd = sc.parallelize(stocks)

# COMMAND ----------

# Pick stocks where close price is less than or equal to 100 , stock[4] is close price
# stock is tuple
rdd2 = rdd.filter (lambda stock: stock[4] <= 100)
rdd2.collect()

# COMMAND ----------

# find gain for the data
# take close price - open price
rdd3 = rdd.map (lambda stock: stock[4] - stock[1])
rdd3.collect()

# COMMAND ----------

# find gain for the data, but include all the results, 
# also include gain as one element
# tuple doesn't support mutation
# we have to create new tuple and include diff
rdd4 = rdd.map (lambda stock: (stock[0], stock[1], stock[2], 
                               stock[3], stock[4], 
                               stock[4] - stock[1] # diff
                              ))
                
rdd4.collect()

# COMMAND ----------

# add two tuples possible, it will create new tuple
(1,2,3) + ('a', 'b', 'c')

# COMMAND ----------

# find gain for the data, but include all the results, 
# also include gain as one element
# tuple doesn't support mutation
# we have to create new tuple and include diff
# refactor
# COMMA , is MUST if we make tuple with 1 element in Python
rdd4 = rdd.map (lambda stock: stock + (stock[4] - stock[1],))
                
rdd4.collect()

# COMMAND ----------


# Databricks notebook source
# joins ,

sectors = [
    # tuple, the key is used for join purpose
    # sector id, sector name
    (0, "AUTOMOTIVE"),
    (1, "TEXTILE"),
    (2, "IT"),
    (4, "MANUFACTURING")
]

stocks = [
    # key   is used for join purpose
    
    # key is 0, 1, 4 and value is  ("TSLA", 100), SYM1, SYM2
    # sector id, symbol, price
    (0, ("TSLA", 100) ),
    (0, ("GM", 40 ) ),
    (1, ("SYM1", 45) ),
    (4, ("SYM2", 67) )
]

sectorRdd = sc.parallelize(sectors)
stocksRdd = sc.parallelize(stocks)
# join based on rdd keys it has to match sectorRdd sector id with stocksRdd sector id
joinRdd = sectorRdd.join(stocksRdd)

joinRdd.collect()

# COMMAND ----------


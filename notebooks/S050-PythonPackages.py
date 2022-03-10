# Databricks notebook source
import simplejson

# COMMAND ----------

data = """
{"stock": "INFY",
 "price": 100.05
}
"""

symbol = simplejson.loads(data)
print(symbol["stock"])

# COMMAND ----------


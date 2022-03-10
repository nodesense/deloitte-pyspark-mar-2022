# Databricks notebook source
# /FileStore/tables/stocks/daily-bhav/cm09MAR2022bhav.csv

stockDf = spark.read.format("csv")\
          .option("header", True)\
          .option("inferSchema", True)\
          .load("/FileStore/tables/stocks/daily-bhav")\
          .drop("_c13")

stockDf.printSchema()
stockDf.show(2)



sectorDf = spark.read.format("csv")\
          .option("header", True)\
          .option("inferSchema", True)\
          .load("/FileStore/tables/stocks/sectors/")


sectorDf.printSchema()
sectorDf.show(2)

# COMMAND ----------

joinDf = stockDf.join(sectorDf, stockDf['SYMBOL'] == sectorDf['SYMBOL'], "inner")
joinDf.printSchema()
joinDf.show(2)

# COMMAND ----------

from pyspark.sql.functions import sum, col, desc
joinDf.groupBy("Industry")\
      .agg(sum (col("TOTTRDVAL")).alias("total_value"))\
      .sort(desc("total_value"))\
      .show(truncate = False)

# COMMAND ----------


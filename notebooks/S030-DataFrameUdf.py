# Databricks notebook source
data = [ 
 ('iPhone', 1000.34),
 ('Galaxy', 600.99),
 ('REdme', 299.99)
]

productDf = spark.createDataFrame(data=data, schema=['name', 'price'])
productDf.printSchema()
productDf.show()

# COMMAND ----------

# Use existing build in functions
from pyspark.sql.functions import upper, lower, floor, ceil, col

df2 = productDf.withColumn("name_upper", upper(col("name")))\
               .withColumn("ceil_price", ceil(col("price")))

df2.printSchema()
df2.show()


# COMMAND ----------

# use prebuild functions sql
productDf.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT name, price, upper(name), ceil(price) from products

# COMMAND ----------

# UDF - User Defined Functions
# custom function developed by developers for special purpose when built functions is not useful.
# value shall be from dataframe, sql statements, where dis is passed by developers
def discount(value, dis):
  total = value - (value * dis / 100.0)
  return total

from pyspark.sql.functions import udf
# create a udf and return udf
discountUdf = udf(discount)

# register the udf in spark session
# "discount" name shall be used in sql SELECT discount(price, 10) from ...
spark.udf.register("discount", discountUdf)


# COMMAND ----------

# try using udf in dataframe python code
from pyspark.sql.functions import lit
df = productDf.withColumn("discounted_price", discountUdf(col("price"), lit(10)))
df.printSchema()
df.show()
 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- discount (price, 10)
# MAGIC SELECT name, price, discount(price, 10) as discounted_price from products

# COMMAND ----------


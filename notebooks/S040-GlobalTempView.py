# Databricks notebook source
products = [ 
          # (product_id, product_name, brand_id)  
         (1, 'iPhone', 100),
         (2, 'Galaxy', 200),
         (3, 'Redme', 300), # orphan record, no matching brand
         (4, 'Pixel', 400),
]

brands = [
    #(brand_id, brand_name)
    (100, "Apple"),
    (200, "Samsung"),
    (400, "Google"),
    (500, "Sony"), # no matching products
]
 
productDf = spark.createDataFrame(data=products, schema=["product_id", "product_name", "brand_id"])
brandDf = spark.createDataFrame(data=brands, schema=["brand_id", "brand_name"])
productDf.show()
brandDf.show()

# COMMAND ----------

# spark context - ONLY ONE PER APPLICATION
# Spark Session - Many Sessions are allowed, ISOLATION against temp views, udf
# temp views and udf created within spark session, only available to that session, not accessible by other session

# create a temp view with in spark session 
productDf.createOrReplaceTempView("products")

# query this products using spark session, this is allowed, products resides inside spark session
spark.sql("SELECT * FROM PRODUCTS").show()


# COMMAND ----------

# create a new spark session
spark2 = spark.newSession() # has isolation on its own view and udf

# query products from spark2, it will fail
# spark2.sql("SELECT * FROM PRODUCTS").show()  # FAIL


# COMMAND ----------

# create a global temp view, which is shared across multiple session

brandDf.createOrReplaceGlobalTempView("brands")

# COMMAND ----------

# multi prefix global_temp. to access global view
spark.sql("SELECT * FROM global_temp.brands").show()

# COMMAND ----------

# now use spark2, which also can access global_temp views
spark2.sql("SELECT * FROM global_temp.brands").show()

# COMMAND ----------


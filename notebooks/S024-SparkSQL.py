# Databricks notebook source
spark.sql("SHOW DATABASES").show()

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS moviedb");

# COMMAND ----------

spark.sql("SHOW DATABASES").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- sql code here
# MAGIC -- this code here shall be wrapped by using spark.sql(), the result df shall be displayed on ui
# MAGIC CREATE TABLE moviedb.reviews(movidId INT, review STRING)

# COMMAND ----------

#dbutils.fs.rm("dbfs:/user/hive/warehouse/moviedb.db/reviews",  True)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO moviedb.reviews VALUES (1, 'nice movie')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM moviedb.reviews

# COMMAND ----------

# MAGIC %sql
# MAGIC -- spark 2.x with delta engine
# MAGIC -- spark 3.x with delta engine [default]
# MAGIC -- we could insert, update, delete the data
# MAGIC 
# MAGIC UPDATE moviedb.reviews set review='avg movie' where movidId = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM moviedb.reviews

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DELETE FROM moviedb.reviews where movidId=1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM moviedb.reviews

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SHOW TABLES IN moviedb

# COMMAND ----------


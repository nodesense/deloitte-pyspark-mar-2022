# Databricks notebook source
# paritionBy

data = [ ("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
   ]

empDf = spark.createDataFrame(data=data, schema=['name', 'dept', 'salary'])
empDf.printSchema()
empDf.show()

empDf.rdd.getNumPartitions()

# COMMAND ----------

empDf.rdd.glom().collect()

# COMMAND ----------

empDf.write.option("header", True)\
  .partitionBy("dept")\
  .csv("/FileStore/tables/employee-partitions")

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# specification for window, partitions, functions that should be applied on partition
# with in department, order the data based on salary in ascending order
windowSpec = Window.partitionBy("dept").orderBy("salary")
# we have apply the spec on dataframe
# row number applied starting from 1 to n, on each partition
# for each partition, row number start from 1
df = empDf.withColumn("row_number", row_number().over(windowSpec))

df.printSchema()
df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank

# rank with gap with ascending order
"""
score  rank
90      1
90      1
89      3  [gap, 2 not included]
"""
windowSpec = Window.partitionBy("dept").orderBy("salary")

df = empDf.withColumn("rank", rank().over(windowSpec))
df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc

# rank with gap
"""
score  rank
90      1
90      1
89      3  [gap, 2 not included]
"""
windowSpec = Window.partitionBy("dept").orderBy(desc("salary"))

df = empDf.withColumn("rank", rank().over(windowSpec))
df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc, col

# rank with gap
"""
score  rank
90      1
90      1
89      3  [gap, 2 not included]
"""
windowSpec = Window.partitionBy("dept").orderBy(desc("salary"))

df = empDf.withColumn("rank", rank().over(windowSpec))\
          .filter (col("rank") == 1)
  
df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, desc

# dense_rank ranking without gap
"""
score  rank
90      1
90      1
89      2  
"""
windowSpec = Window.partitionBy("dept").orderBy(desc("salary"))

df = empDf.withColumn("rank", dense_rank().over(windowSpec))
df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import percent_rank, desc

# percent_rank ranking with perecent calculation
"""
 
"""
windowSpec = Window.partitionBy("dept").orderBy(desc("salary"))

df = empDf.withColumn("percent_rank", percent_rank().over(windowSpec))
df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import ntile, desc

# ntile ranking with related certain range for range
# rank shall fit into a range
"""
 
"""
windowSpec = Window.partitionBy("dept").orderBy(desc("salary"))

df = empDf.withColumn("ntile", ntile(3).over(windowSpec))
df.show()

# COMMAND ----------

# Analytic functions
# Cumulative distribution - similar to rank, calcualted and values are bound between 
# 0 and 1

# 10 USD per share => 13 USD per share      = 3 USD per share, 30 % gain .3
# 100 USD per share => 110 USD per share    = 10 USD per share, 10% gain .1
# cumulative distribution
from pyspark.sql.window import Window
from pyspark.sql.functions import cume_dist, desc

# similar to  rank  
 
windowSpec = Window.partitionBy("dept").orderBy(desc("salary"))

df = empDf.withColumn("cume_dist", cume_dist().over(windowSpec))
df.show()

# COMMAND ----------

# lag - previous lag
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, desc

windowSpec = Window.partitionBy("dept").orderBy("salary")

df = empDf.withColumn("lag", lag("salary",1).over(windowSpec))
df.show()

# COMMAND ----------

# lead -  the one ahead, 
from pyspark.sql.window import Window
from pyspark.sql.functions import lead, desc

windowSpec = Window.partitionBy("dept").orderBy("salary")

df = empDf.withColumn("lead", lead("salary",1).over(windowSpec))
df.show()

# COMMAND ----------


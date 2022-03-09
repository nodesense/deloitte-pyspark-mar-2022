# Databricks notebook source
# spark notebook
# Notebook is a spark application
# each spark application shall have a spark driver component 
# each spark application shall have spark context
# in Databricks notebook, spark context already predefined as variable sc

sc
# shift + enter key to execute cell

# COMMAND ----------

# RDD - Resillient Distributed DataSet
# way of breaking large data into partitions, load them into memory
# RDD - Java/Scala RDD, Python RDD
# RDD is tree
#     Partitions
#     Transformation

# COMMAND ----------

# Create RDD from existing hardcoded list
# load data from spark driver/this notebook into Spark Executor
# located in spark driver/this notebook
data = [1,2,3,4,5,6,7,8,9, 10]

# load this into RDD, ie into partition, into spark executor
# intellisense auto suggession
# sc.<TAB><TAB><TAB>
# Lazy evaluation. This data is not loaded into memory as soon we execute the code below
# this data shall be loaded into memory/paritions/executor until we apply ACTION on top of RDD
# You can treat RDD like a plan for itineray 
# CREATION OF RDD
# RDD is a reference for a data that may be available in future when action applied
rdd = sc.parallelize(data)

# COMMAND ----------

# Transformation
# a piece of code applied on RDD data on parition to tranform, filter, groupby,join, map, sort etc..
# filter - transformation
# odd numbers, filter all the odd number from rdd
# TRANSFORMATION ALSO LAZY EVALUATION, 
# until we apply ACTION, it is just plan, no data loaded into memory, no task, no thread nothing is created/run
# RDD Lineage - Reuse RDD for further operation, rdd is parent  , rdd2 is child
rdd2 = rdd.filter (lambda n : n % 2 == 1)

# COMMAND ----------

# Action
# Action is the one trigger Job Execution, ie run the plan on cluster of machines, write output to 
# to database, to kafka, to s3/hdfc, or get the output back to driver code
# Action need physical resources, when action applied, spark internally create parition, load data, create memory, thread/task, execute the transformation like filter,map,...
# collect is one of the action, that execute the job, collect results back to driver code/notebook
# use less of collect method, only for development purpose
result = rdd2.collect()
print(result)

# COMMAND ----------

# Every time we apply action on RDD, it create a new spark job, load data into memory 
# apply computation
rdd2.collect()

# COMMAND ----------

# Every time we apply action on RDD, it create a new spark job, load data into memory 
rdd2.collect()

# COMMAND ----------

# apply another transformation ie multiple the odd number with 10
# lazy evaluation, not creating any job
# transformation, you can see no job created
rdd3 = rdd2.map (lambda n : n * 10)

# COMMAND ----------

rdd3.collect()

# COMMAND ----------


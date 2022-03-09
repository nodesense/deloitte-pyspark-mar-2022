# Databricks notebook source
# read movies.txt we already uploaded
# skip the header line
# parse the content into tuple (1, 'Toy Story', 'ANIMATION')
# /FileStore/tables/ml-latest-small/movies.csv

moviesFileRdd = sc.textFile("/FileStore/tables/ml-latest-small/movies.csv")

# COMMAND ----------

header = moviesFileRdd.first() # read first line

moviesRdd = moviesFileRdd.filter (lambda line: line != header)\
                         .map (lambda line: tuple(line.split(",")))\
                         
moviesRdd.take(5)

# COMMAND ----------

# t[0:2] picking 0, 1st element, movieid, movie title
moviesRdd = moviesRdd.map (lambda t: t[0:2] + tuple(t[2].split("|")))

moviesRdd.take(3)

# COMMAND ----------

# Learn regular expression, see how can you extract the year as one element
# TODO
# Databricks notebook source
# find unique word count in text file
# create a  file notepad, save it as words.txt in your desktop
# upload file into databricks data 

fileRdd = sc.textFile("/FileStore/tables/mar-07-2022/words.txt")

# COMMAND ----------

# action
fileRdd.count()  

# COMMAND ----------

# collect
fileRdd.collect()

# COMMAND ----------

# map, read line by line, convert to lower case, strip the spaces around
# transformation
lowerCaseRdd = fileRdd.map (lambda line: line.lower().strip())

# COMMAND ----------

lowerCaseRdd.collect()

# COMMAND ----------

# break line into array/list of words, 
wordListRdd = lowerCaseRdd.map (lambda line: line.split(" "))

# COMMAND ----------

print(wordListRdd.count())
wordListRdd.collect()

# COMMAND ----------

# flatMap - Transformation
# this will flatten a list, remove the list, project only elements inside list as record/output
# Input words: ['databricks', 'spark']
# output
# databricks
# spark
wordsRdd = wordListRdd.flatMap(lambda words : words)

# COMMAND ----------

print(wordsRdd.count())
wordsRdd.collect()

# COMMAND ----------

# remove all the empty strings
notEmptyWordsRdd = wordsRdd.filter (lambda word: word != '')
print(notEmptyWordsRdd.count())
notEmptyWordsRdd.collect()

# COMMAND ----------

# until this point, we did transformation, clean the file content, prepare the data for analytics
# ---------------------------------
# create a word pair rdd, using tuple
# PairRdd (key, value), is made from tuple, first member in tuple is a key, second one is value
# spark => (spark, 1)
# return tuple with (spark, 1)
wordPairRdd = notEmptyWordsRdd.map (lambda word: (word, 1))

# COMMAND ----------

wordPairRdd.collect()

# COMMAND ----------

# analytics, transformation
# reduceByKey, keys is actual work like spark, apache
# count unique keys ie word
"""
Input:
('spark', 1), <- first time spark appear, put on the table state, won't call reducer lambda
 ('kafka', 1),  <- first time kafka appear, put on the table state, won't call reducer lambda
 ('kafka', 1),<- second time kafka appear, call reducer lambda taking acc from table/prev result, pass the value to lambda
                    lambda acc, value = acc(1), value (1) from (pair value), acc + value = 2,
                     result 2 is updated in table for kafka
 ('spark', 1), <- second time spark appear, call reducer lambda taking acc from table/prev result, pass the value to lambda
                    lambda acc, value = acc(1), value (1) from (pair value), acc + value = 2,
                     result 2 is updated in table for spark
 ('apache', 1),  <- first time apache appear, put on the table state, won't call reducer lambda
 ('spark', 1),<- 3rd time spark appear, call reducer lambda taking acc from table/prev result, pass the value to lambda
                    lambda acc, value = acc(2), value (1) from (pair value), acc + value = 3,
                     result 3 is updated in table for spark
 ('databricks', 1),
 ('databricks', 1),
 ('spark', 1), <- 4th  time spark appear, call reducer lambda taking acc from table/prev result, pass the value to lambda
                    lambda acc, value = acc(3), value (1) from (pair value), acc + value = 4,
                     result 4 is updated in table for spark
 ('apple', 1),
 ('orange', 1)

reduceByKey: (lambda acc, value: acc + value)

State internal to reduceByKey, assume a table like structure
acc - accumulator 

word    acc
spark   4
kafka   2
apache   1
databricks 2
apple 1
orange 1

 
"""
wordCountRdd = wordPairRdd.reduceByKey(lambda acc, value: acc + value)
wordCountRdd.collect()

# COMMAND ----------


# Databricks notebook source
from pyspark import StorageLevel


# COMMAND ----------

# how to remove special chars
# we will all ascii
def to_ascii(text):
    import re
    output = re.sub(r"[^a-zA-Z0-9 ]","",text)
    #print(output)
    return output

text = "prince, don't ;welcome!"
to_ascii(text)

# COMMAND ----------

# https://raw.githubusercontent.com/mmcky/nyu-econ-370/master/notebooks/data/book-war-and-peace.txt
wordCountRdd = sc.textFile("/FileStore/tables/mar-07-2022/book_war_and_peace.txt")\
                 .map (lambda line: to_ascii(line))\
                  .map (lambda line: line.strip().lower())\
                 .map (lambda line: line.split(" "))\
                 .flatMap(lambda elements: elements)\
                 .filter (lambda word: word != "")\
                 .map (lambda word: (word, 1))\
                 .reduceByKey(lambda acc, value: acc + value)


wordCountRdd.cache() # , call persit internally with MEMORY_ONLY

#wordCountRdd.persist(StorageLevel.MEMORY_AND_DISK) # Some data shall be in memory, some shall be stored into disk
#wordCountRdd.persist(StorageLevel.DISK_ONLY) # All data in disk only
#wordCountRdd.persist(StorageLevel.MEMORY_ONLY_2) # store cache in replica memory.. 
wordCountRdd.take(10)

# COMMAND ----------

# sort data by value, in ascending order
# least used words
# wordCountRdd is cached already, it won't read text file again and again for every action or reusable rdd
sortedRddAscending = wordCountRdd.sortBy(lambda kv: kv[1]) # kv[1] is word count, not the word

sortedRddAscending.take(10)

# COMMAND ----------

# sortBy values but decending order
# most used words
# we reuse rdd, but the data shall be taken cache
print(wordCountRdd.getNumPartitions())
sortedRddDecending = wordCountRdd.sortBy(lambda kv: kv[1], ascending=False)
sortedRddDecending.take(10)

# COMMAND ----------

# saveAsTextFile is action, write the results into a text file
# we resuse wordCountRdd
# word-count-results is a folder, shall have 2 paritions data in there
wordCountRdd.saveAsTextFile("/FileStore/tables/mar-07-2022/word-count-results")


# COMMAND ----------

# we resuse wordCountRdd
sortedRddDecending.saveAsTextFile("/FileStore/tables/mar-07-2022/word-count-results-desc")


# COMMAND ----------


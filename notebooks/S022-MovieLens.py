# Databricks notebook source
# Databricks notebook source
from pyspark.sql.types import StructType, LongType,StringType, IntegerType, DoubleType


# COMMAND ----------

# create dataframe schema manually, best practices
 
movieSchema = StructType()\
         .add("movieId", IntegerType(), True)\
         .add("title", StringType(), True)\
         .add("genres", StringType(), True)

# COMMAND ----------


ratingSchema = StructType()\
         .add("userId", IntegerType(), True)\
         .add("movieId", IntegerType(), True)\
         .add("rating", DoubleType(), True)\
         .add("timestamp", StringType(), True)


# COMMAND ----------

#inferSchema, spark scan records and build schema with data types, expensive due file io
# movieDf = spark.read.format("csv")\
#           .option("header", True)\
#           .option("inferSchema", True)\
#           .load("/FileStore/tables/ml-latest-small/movies.csv")
# movieDf.printSchema()
# movieDf.show(2)

# COMMAND ----------

movieDf = spark.read.format("csv")\
          .option("header", True)\
          .schema(movieSchema)\
          .load("/FileStore/tables/ml-latest-small/movies.csv")

movieDf.printSchema()
movieDf.show(2)

# COMMAND ----------

ratingDf = spark.read.format("csv")\
          .option("header", True)\
          .schema(ratingSchema)\
          .load("/FileStore/tables/ml-latest-small/ratings.csv")

ratingDf.printSchema()
ratingDf.show(2)

# COMMAND ----------

print(movieDf.count())
print(ratingDf.count())

# COMMAND ----------

# title, genres columns are truncated with ...
movieDf.show()  # 20 records

# COMMAND ----------

# not to truncate columns while displaying
movieDf.show(truncate = False)

# COMMAND ----------

# to get all columns
print("Columns", ratingDf.columns)
# to get schema associated with dataframe
print("Schema ", ratingDf.schema)

# COMMAND ----------


# add new columns/drive new columns from existing data
df3 = ratingDf.where("rating < 2").withColumn("rating_adjusted", ratingDf.rating + .2  )
df3.printSchema()
df3.show(2)
print("derived ", df3.count())


# COMMAND ----------

df2 = ratingDf.withColumnRenamed("rating", "ratings")
df2.printSchema()
df2.show(2)

# COMMAND ----------


df2 = ratingDf.select(ratingDf.userId, 
                     (ratingDf.rating + 0.2).alias("rating_adjusted") )
df2.show(1)

# COMMAND ----------

# filter with and conditions
df2 = ratingDf.filter( (ratingDf.rating >=3) & (ratingDf.rating <=4))
df2.show(4)

# COMMAND ----------


from pyspark.sql.functions import col, asc, desc
# sort data by ascending order/ default
df2 = ratingDf.sort("rating")
df2.show(5)
# sort data by ascending by explitly
df2 = ratingDf.sort(asc("rating"))
df2.show(5)
# sort data by descending order
df2 = ratingDf.sort(desc("rating"))
df2.show(5)

# COMMAND ----------

# aggregation count
from pyspark.sql.functions import col, desc, avg, count
# count, groupBy
# a movie, rated by more users, dones't count avg rating
# filter, ensure that total_ratings >= 100 users
mostPopularDf = ratingDf\
                .groupBy("movieId")\
                .agg(count("userId"))\
                .withColumnRenamed("count(userId)", "total_ratings")\
                .sort(desc("total_ratings"))\
                .filter(col("total_ratings") >= 100)


mostPopularDf.show(5)

# COMMAND ----------

# aggregation count
from pyspark.sql.functions import col, desc, avg, count
# count, groupBy
# a movie, rated by more users, dones't count avg rating
# filter, ensure that total_ratings >= 100 users
mostPopularDf = ratingDf\
                .groupBy("movieId")\
                .agg(count("userId"), avg("rating").alias("avg_rating"))\
                .withColumnRenamed("count(userId)", "total_ratings")\
                .sort(desc("total_ratings"))\
                .filter(col("total_ratings") >= 100)


mostPopularDf.show(5)

# COMMAND ----------

# join popular movies with movies df to get title, genres
mostPopularMovies = mostPopularDf.join (movieDf, mostPopularDf.movieId == movieDf.movieId)\
                                  .drop(movieDf.movieId)\
                                  .select("movieId", "title", "avg_rating", "total_ratings", "genres")\
                                  .sort(desc("total_ratings"))
        

mostPopularMovies.printSchema()
mostPopularMovies.show(3)

# COMMAND ----------

# cache the data
mostPopularMovies.cache()

# COMMAND ----------

# write the result into parquet binary, column based format
mostPopularMovies.write.mode('overwrite')\
                         .parquet("/FileStore/tables/movielens-results/movielens-parquet")


# COMMAND ----------

# write the result into orc binary, column based format
mostPopularMovies.write.mode('overwrite')\
                         .orc("/FileStore/tables/movielens-results/movielens-orc")


# COMMAND ----------

# write the result into json text, row based format
mostPopularMovies.write.mode('overwrite')\
                         .json("/FileStore/tables/movielens-results/movielens-json")


# COMMAND ----------

# write the result into csv text, row based format
mostPopularMovies.write.mode('overwrite')\
                         .option("header", True)\
                         .csv("/FileStore/tables/movielens-results/movielens-csv")


# COMMAND ----------

# read the result from csv file result
popularMoviesCsvDf = spark.read.format("csv")\
          .option("header", True)\
          .option("inferSchema", True)\
          .load("/FileStore/tables/movielens-results/movielens-csv")

popularMoviesCsvDf.printSchema()
popularMoviesCsvDf.show(2)

# COMMAND ----------

# read the result from json file result
# no header for json
popularMoviesJsonDf = spark.read.format("json")\
          .option("inferSchema", True)\
          .load("/FileStore/tables/movielens-results/movielens-json")

popularMoviesJsonDf.printSchema()
popularMoviesJsonDf.show(2)

# COMMAND ----------

# read the result from parquet file result
# no header for parquet
# parquet has column/schema itself, no need to inferSchema for parquet format
popularMoviesParquetDf = spark.read.format("parquet")\
          .load("/FileStore/tables/movielens-results/movielens-parquet")

popularMoviesParquetDf.printSchema()
popularMoviesParquetDf.show(2)

# COMMAND ----------

# read the result from orc file result
# no header for orc
# orc has column/schema itself, no need to inferSchema for orc format
popularMoviesOrcDf = spark.read.format("orc")\
          .load("/FileStore/tables/movielens-results/movielens-orc")

popularMoviesParquetDf.printSchema()
popularMoviesParquetDf.show(2)

# COMMAND ----------


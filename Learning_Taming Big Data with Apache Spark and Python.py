# Databricks notebook source
# Read the SQL table into Spark DataFrames
yellow_trip_df = spark.read.table("lombov1.default.yellow_tripdata_2022_01")

# Creat SQL table yellow_trip
yellow_trip_df.createOrReplaceTempView("yellow_trip")

# SQL can be run over DF that have been registered as a SQL table
vendor_df = spark.sql("select distinct VendorID from yellow_trip")
PULocationID_df = spark.sql("select PULocationID,count(*) from yellow_trip group by PULocationID order by PULocationID").show()

# Apply common operations
yellow_trip_df.groupBy('PULocationID').count().orderBy('PULocationID').show()

# Apply df collect() operation
for vendor in vendor_df.collect():
    print(vendor['VendorID'])

# COMMAND ----------

print('Here is our schema:')
yellow_trip_df.printSchema()

print("Let's display the VendorID:")
yellow_trip_df.select('VendorID').show()

print("Filter to VendorID = 2")
yellow_trip_df.filter(yellow_trip_df.VendorID == 2).show()

print("Group by VendorID")
yellow_trip_df.groupBy(yellow_trip_df.VendorID).count().show()

print("Add 100 dollars to tips")
Vendor_tip_df = yellow_trip_df \
                .withColumn("tip_amount", yellow_trip_df.tip_amount.cast("double")) \
                .groupBy("VendorID").sum("tip_amount") \
                .withColumnRenamed("sum(tip_amount)", "total_tip_amount")
# Vendor_tip_df.show()
Vendor_tip_df \
            .selectExpr("VendorID", "total_tip_amount", "total_tip_amount + 100 as made_up_tip_amount") \
            .sort('made_up_tip_amount', ascending=False) \
            .show()


# COMMAND ----------

from pyspark.sql import functions as func

print('Here is our schema:')
yellow_trip_df.printSchema()

print("Avg trips using function")
Vendor_tip_df = yellow_trip_df \
                .withColumn("tip_amount", yellow_trip_df.tip_amount.cast("double")) \
                .groupBy("VendorID") \
                .agg(func.round(func.sum("tip_amount"),2)) \
                .alias("total_tip_amount") \
                .sort("VendorID",ascending=False)
Vendor_tip_df.show()


# COMMAND ----------

from pyspark.sql import functions as func

# Read book.txt into a dataframe
inputDF = spark.read.table("lombov1.default.test")
# inputDF.show()

# Split using a regular expression that extracts words
words_df = inputDF.select(func.explode(func.split(inputDF.value,"\\W+")).alias("word"))
words_noEmptyStr_df = words_df.filter(words_df.word != '')

# Normalize everything to lowercase
lowercase_df = words_noEmptyStr_df.select(func.lower("word").alias("lowercase_word"))

# Count num of occurance of each word and sorted
wordcounts_df = lowercase_df.groupBy("lowercase_word").count().sort('count',ascending=False)

# show top 20
wordcounts_df.show()

# show all
wordcounts_df.show(wordcounts_df.count())

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

schema = StructType([ \
                    StructField("stationID", StringType(), True), \
                    StructField("date", IntegerType(), True), \
                    StructField("measure_type", StringType(), True), \
                    StructField("temperature", FloatType(), True)                   
                    ])

# Read csv file as dataframe and apply custom schema
# df = spark.read.schema(schema).csv("1800.csv")
# df.printSchema()

df = spark.read.schema(schema).table("lombov1.default.test")
min_temp_df = df \
            .filter(df.measure_type == 'TMIN') \
            .select(df.stationID, df.temperature) \
            .groupBy(df.stationID) \
            .min("temperature") \
            .withColumnRenamed("min(temperature)","min_temp")


# add a new column and use func.col or selectExpr
temp_F_df = min_temp_df \
        .withColumn('temperature', func.round(func.col('min_temp') * 0.1 * (9.0/5.0) + 32.0,2)) \
        .select('stationID','temperature') \
        .sort('temperature') \
        # .show()

temp_F_df2 = min_temp_df \
        .selectExpr('stationID','min_temp * 0.1 * (9.0/5.0) + 32.0 as temperature') \
        .sort('temperature') \
        # .show()


# print the result
results = temp_F_df.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))


# COMMAND ----------

# filename = 'customer-orders.csv'

from pyspark.sql.types import StructType, StructField, StringType, FloatType

schema = StructType([ \
                    StructField('customerID', StringType(), True),
                    StructField('itemID', StringType(), True),
                    StructField('Amount', FloatType(), True)
                    ])

df = spark.read.schema(schema).table('lombov1.default.test')

df \
    .groupBy('customerID') \
    .agg(func.round(func.sum('Amount'),2)) \
    .withColumnRenamed('round(sum(Amount), 2)','total_spent') \
    .sort('total_spent') \
    .show()



# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType

df = spark.read.schema(schema).table('lombov1.default.test')
df.printSchema()

# changing the datatype of a dataframe using cast
df = df \
        .withColumn('user_id',df.user_id.cast(IntegerType())) \
        .withColumn('item_id',df.item_id.cast(IntegerType())) \
        .withColumn('rating',df.rating.cast(IntegerType())) \
        .withColumn('timestamp',df.timestamp.cast(LongType()))
df.printSchema()

df.groupBy('item_id').count().sort('count',ascending=False).show()
df.groupBy('item_id').count().orderBy('count',ascending=False).show()

# COMMAND ----------

# filename = 'Marvel_Graph.txt' & 'Marvel_Names.txt'

connections_df = spark.read.table('lombov1.default.test')
names_df = spark.read.table('lombov1.default.test2')

# split a column into multiple limited column
splitcol = func.split(names_df['value'],' ',limit=2)
names_df = names_df \
            .withColumn('id',splitcol[0]) \
            .withColumn('name',splitcol[1]) \
            .drop('value')
# names_df.show()

# get id and number of connections
splitcol = func.split(connections_df['value'], ' ')
connections_df = connections_df \
                .withColumn('id', splitcol[0]) \
                .withColumn('connections',func.size(splitcol)-1) \
                .drop('value') \
                .groupBy('id') \
                .agg(func.sum('connections').alias('connections'))

sort_df = connections_df.orderBy('connections',ascending=False)

# pick first id & name
mostPopular = sort_df.first()
mostPopularName = names_df \
                    .filter(names_df.id == mostPopular['id']).first()

print(mostPopularName['name'] + ' is the most popular superhero with ' + str(mostPopular['connections']) + ' co-appearances')

# COMMAND ----------

# Find the most obscure superheros

# min connection
min_connection = connections_df.agg(func.min('connections')).first()[0]
# print(min_connection)

# with min connection
obscure_sh = connections_df.filter(connections_df.connections == min_connection)

obscure_sh = obscure_sh.join(names_df,'id')

print('The following superhearos have only ' + str(min_connection) + ' connections')
obscure_sh.select('name').show()


# COMMAND ----------

import sys

# movie-similarities-search
def computeCosineSimilarity(spark, data):
    # Compute xx, xy and yy columns
    pairScores = data \
      .withColumn("xx", func.col("rating1") * func.col("rating1")) \
      .withColumn("yy", func.col("rating2") * func.col("rating2")) \
      .withColumn("xy", func.col("rating1") * func.col("rating2")) 

    # Compute numerator, denominator and numPairs columns
    calculateSimilarity = pairScores \
      .groupBy("movie1", "movie2") \
      .agg( \
        func.sum(func.col("xy")).alias("numerator"), \
        (func.sqrt(func.sum(func.col("xx"))) * func.sqrt(func.sum(func.col("yy")))).alias("denominator"), \
        func.count(func.col("xy")).alias("numPairs")
      )

    # Calculate score and select only needed columns (movie1, movie2, score, numPairs)
    result = calculateSimilarity \
      .withColumn("score", \
        func.when(func.col("denominator") != 0, func.col("numerator") / func.col("denominator")) \
          .otherwise(0) \
      ).select("movie1", "movie2", "score", "numPairs")

    return result

# Get movie name by given movie id 
def getMovieName(movieNames, movieId):
    result = movieNames.filter(func.col("movie_id") == movieId) \
        .select("movie_title").collect()[0]

    return result[0]


movieNamesSchema = StructType([ \
                               StructField("movieID", IntegerType(), True), \
                               StructField("movieTitle", StringType(), True) \
                               ])
    
moviesSchema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])
    
    
# Create a broadcast dataset of movieID and movieTitle.
# # Apply ISO-885901 charset
# movieNames = spark.read \
#       .option("sep", "|") \
#       .option("charset", "ISO-8859-1") \
#       .schema(movieNamesSchema) \
#       .csv("file:///SparkCourse/ml-100k/u.item")

# # Load up movie data as dataset
# movies = spark.read \
#       .option("sep", "\t") \
#       .schema(moviesSchema) \
#       .csv("file:///SparkCourse/ml-100k/u.data")

movieNames = spark.read.table('lombov1.default.test2')
movies = spark.read.table('lombov1.default.test')

ratings = movies.select("userId", "movieId", "rating")

# Emit every movie rated together by the same user.
# Self-join to find every combination.
# Select movie pairs and rating pairs
moviePairs = ratings.alias('ratings1') \
                .join(ratings.alias('ratings2'), (func.col('ratings1.userId') == func.col('ratings2.userId')) \
                    & (func.col('ratings1.movieId') < func.col('ratings2.movieId'))) \
                .select(func.col('ratings1.movieId').alias('movie1'), \
                        func.col('ratings2.movieId').alias('movie2'), \
                        func.col('ratings1.rating').alias('rating1'), \
                        func.col('ratings2.rating').alias('rating2')
                        )

# cache the result
moviePairSimilarities = computeCosineSimilarity(spark, moviePairs).cache()

if (len(sys.argv) > 1):
    scoreThreshold = 0.97
    coOccurrenceThreshold = 100.0

    movieID = 50

    # Filter for movies with this sim that are "good" as defined by
    filteredResults = moviePairSimilarities \
                        .filter( \
                        ((func.col('movie1') == movieID) | (func.col('movie2') == movieID)) \
                        & (func.col('score') > scoreThreshold) \
                        & (func.col('numPairs') > coOccurrenceThreshold) \
                        )
    

    # Sort by quality score.
    results = filteredResults.orderBy('score',ascending=False).take(10)
    
    print ("Top 10 similar movies for " + getMovieName(movieNames, movieID))
    
    for result in results:
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = result.movie1
        if (similarMovieID == movieID):
          similarMovieID = result.movie2
        
        print(getMovieName(movieNames, similarMovieID) + "\tscore: {:.02f}".format(result.score) \
               + "\tstrength: " + str(result.numPairs))
        

# COMMAND ----------



import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW
"""
Reviews = "[[reviews], [dates]]" - find a way to split these
"""

csv_file_path = "hdfs://%s:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv" % hdfs_nn
df2 = spark.read.csv(csv_file_path, header= True, inferSchema=True)

reviews_and_dates = split(df2["Reviews"],"\\], \\[")
df2 = df2.withColumn("review",reviews_and_dates.getItem(0)).withColumn("date",reviews_and_dates.getItem(1))
# add into dataframe
df2 = df2.withColumn("review", split(df2["review"], "\\', \\'")).withColumn("date", split(df2["date"], "\\', \\'"))
# now is multiple reviews and dates in 1 line -> split again

"""
put all the reviews and corr. dates together
explode into new rows with key = review, value = corr. date
create new dataframe with the 3 columns
"""
new_df = df2.withColumn("new", arrays_zip("review", "date")).withColumn("new", explode("new")).select("ID_TA", col("new.review").alias("review"), col("new.date").alias("date"))

#remove inverted commas and square brackets
new_df = new_df.withColumn("review", regexp_replace("review", "'", "")).withColumn("date", regexp_replace("date", "'", ""))
new_df = new_df.withColumn("review", regexp_replace("review", "\\[", "")).withColumn("date", regexp_replace("date", "\\]", ""))

# remove leading/trailing whitespace
new_df = new_df.withColumn("review", trim(new_df.review)).withColumn("date", trim(new_df.date))
#new_df.show()

new_df.write.csv("hdfs://%s:9000/assignment2/output/question3/" % hdfs_nn, header=True)
spark.read.csv("hdfs://%s:9000/assignment2/output/question3/" % hdfs_nn ,header=True,inferSchema=True).show()
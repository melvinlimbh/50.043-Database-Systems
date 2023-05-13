import sys
from pyspark.sql import SparkSession

# you may add more import if you need to
from pyspark.sql.functions import regexp_replace, split, explode, col, trim, count

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW
df = (
    spark.read.option("header", True)
    .option("delimiter", ",")
    .option("inferSchema", True)
    .option("quotes", '"')
    .csv("hdfs://%s:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv" % hdfs_nn)
)

# remove square brackets
df = df.withColumn("Cuisine Style", regexp_replace("Cuisine Style", "\\[", "")).withColumn("Cuisine Style", regexp_replace("Cuisine Style", "\\]", ""))

# split
df = df.withColumn("Cuisine Style", split(col("Cuisine Style"), ", "))

df_explode = df.withColumn("Cuisine Style", explode("Cuisine Style")).withColumn("Cuisine Style", regexp_replace("Cuisine Style", "'", "")).withColumn("Cuisine Style", trim(col("Cuisine Style"))).withColumn("Cuisine", regexp_replace("Cuisine Style", "'", ""))

new_df = df_explode.select("City", "Cuisine")

new_df = new_df.groupBy("City", "Cuisine").count()

new_df = new_df.select(
    col("City"),
    col("Cuisine"),
    col("count")
)
new_df.write.csv("hdfs://%s:9000/assignment2/output/question4/" % hdfs_nn, header=True)
new_df.show()
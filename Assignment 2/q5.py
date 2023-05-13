import sys 
from pyspark.sql import SparkSession
# you may add more import if you need to
from pyspark.sql.functions import from_json, explode, col, array, array_sort
from pyspark.sql.types import ArrayType, StructType, StructField, StringType

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW
schema = ArrayType(StructType([
    StructField("name", StringType())
]))

df = (
    spark.read.option("header", True)
    .option("delimiter", ",")
    .option("inferSchema", True)
    .option("quotes", '"')
    .parquet("hdfs://%s:9000/assignment2/part2/input/tmdb_5000_credits.parquet" % hdfs_nn)
)

df = df.drop("crew")

df = df.withColumn("actor1", explode(from_json(col("cast"), schema).getField("name"))).withColumn("actor2", explode(from_json(col("cast"), schema).getField("name")))

df = df.select("movie_id", "title", "actor1", "actor2").filter(col("actor1") != col("actor2"))

df = df.withColumn("actors", array(col("actor1"), col("actor2"))).withColumn("actors", array_sort(col("actors")).cast("string"))

df = df.dropDuplicates(["movie_id", "title", "actors"]).sort(col("actors").asc())

df_counter = (
    df.groupBy("actors")
    .count()
    .filter(col("count") > 1)
)

new_df = (
    df_counter.join(df, ["actors"], "inner")
    .sort(col("actors").asc())
    .drop("actors", "count")
)

new_df.show()

new_df.write.option("header", True).mode("overwrite").parquet(
    "hdfs://%s:9000/assignment2/output/question5/" % (hdfs_nn)
)

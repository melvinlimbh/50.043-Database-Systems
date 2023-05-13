import sys
from pyspark.sql import SparkSession
# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW
csv_file_path ="hdfs://%s:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv" % hdfs_nn

df2 = spark.read.csv(csv_file_path, header= True, inferSchema=True)

df2 = df2.filter((df2["Reviews"] != "[ [  ], [  ] ]")).filter(df2["Reviews"].isNotNull())
df2 = df2.filter((df2["Rating"] >= 1.0))
df2.write.csv("hdfs://%s:9000/assignment2/output/question1/" % hdfs_nn, header=True)
spark.read.csv("hdfs://%s:9000/assignment2/output/question1/" % hdfs_nn,header=True, inferSchema= True).show()
from pyspark.sql import SparkSession

spark = SparkSession.builder \
  .master("local[2]") \
  .appName("Hello PySpark") \
  .getOrCreate()

dataList = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
df = spark.sparkContext.parallelize(dataList) \
    .toDF(["Lang", "Score"])

df.show()

spark.stop()


from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = (
    spark.read.format("xml")
    .options(rowTag="book")
    .load("s3://data-lake-kraisfeld/books.xml")
)

df.write.format("parquet").mode("overwrite").save("s3://data-lake-kraisfeld/newbooks")

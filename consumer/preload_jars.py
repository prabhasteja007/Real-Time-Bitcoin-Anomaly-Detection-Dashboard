from pyspark.sql import SparkSession
ss = SparkSession.builder \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()
ss.stop()
print("Kafka connector JAR downloaded successfully.")

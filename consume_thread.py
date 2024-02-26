import json
import threading
import time

from kafka import KafkaConsumer
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.session import SparkSession
from pyspark.sql.types import (DateType, IntegerType, StringType, StructField,
                               StructType)

# Configuration du consommateur Kafka
KAFKA_BROKER_URL = "localhost:9092"
TOPIC_NAME = "IA"

spark = SparkSession.builder.appName("TP Note").config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0').getOrCreate()
df: DataFrame = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", KAFKA_BROKER_URL) \
  .option("subscribe", TOPIC_NAME) \
  .load()

schema = StructType([
  StructField("id", IntegerType(), True),
  StructField("date", DateType(), True)
])

# df extract from json
res = df.select(from_json(col("value").cast("string"), schema))
# res.dropDuplicates(["id"])

res.writeStream.format("console").outputMode("append").start().awaitTermination()

#df.writeStream().format("console").outputMode("append").start().awaitTermination()
#df.write.csv('messages.csv')

# df.stop()



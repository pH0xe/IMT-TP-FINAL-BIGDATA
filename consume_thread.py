from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, row_number, window
from pyspark.sql.session import SparkSession
from pyspark.sql.types import (DateType, LongType, StructField, StringType,TimestampType,
                               StructType)

# Configuration du consommateur Kafka
KAFKA_BROKER_URL = "localhost:9092"
TOPIC_NAME = "IA"

builder: SparkSession.Builder = SparkSession.builder
spark: SparkSession = builder \
    .appName("TP Note") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
    .getOrCreate()

df: DataFrame = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER_URL) \
    .option("subscribe", TOPIC_NAME) \
    .option("failOnDataLoss", "false") \
    .load()

schema = StructType([
    StructField("id", StringType(), True),
    StructField("date", TimestampType(), True)
])

df = df.withColumn(
    "value",
    from_json(col("value").cast("string"), schema)
)
df = df.select(col("value.*"))
window_spec = window("date", "6 hours", "30 minutes") 

df = df.withColumn("range_start", window_spec.start)
df = df.withColumn("range_end", window_spec.end)


# On rajoute un watermark pour spécifier la durée de vie des données sur la colonne range_start
df = df.withWatermark("range_start", "6 hours")

# on groupe par la fenêtre et on compte le nombre de threads
df = df.groupBy("range_start", "range_end").count()


# df.writeStream \
#     .format("csv") \
#     .trigger(processingTime="10 seconds") \
#     .option("path", "output") \
#     .option("checkpointLocation", "checkpoint") \
#     .outputMode("append") \
#     .start() \
#     .awaitTermination()

query = df.writeStream \
  .format("memory") \
  .trigger(processingTime="10 seconds") \
  .queryName("temp") \
  .outputMode("complete")\
  .start()

# On dort jusqu'a ce que l'utilisateur demande d'arreter
# une fois que l'utilisateur demande d'arreter, on arrete le stream, et on save tous dans un fichier
input("\n\nPress any key to stop\n\n")
print("\n\nStopping the stream...")
query.stop()

# On sauvegarde le résultat dans un fichier
result = spark.sql("SELECT * FROM temp")
result.show()
result.coalesce(1).sort(['range_start', 'range_end']).write.csv("unique_output", mode="overwrite", header=True)



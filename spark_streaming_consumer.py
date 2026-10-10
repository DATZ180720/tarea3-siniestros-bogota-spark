"""Consume eventos viales sintéticos de Kafka y cuenta eventos por ventana."""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


KAFKA_SERVERS = "localhost:9092"
TOPIC = "siniestros_bogota"

schema = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_time", TimestampType()),
        StructField("locality", StringType()),
        StructField("accident_class", StringType()),
        StructField("severity", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("synthetic", BooleanType()),
    ]
)

spark = SparkSession.builder.appName("KafkaSparkStreamingSiniestros").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

kafka_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

events = (
    kafka_stream.select(F.from_json(F.col("value").cast("string"), schema).alias("event"))
    .select("event.*")
    .filter(F.col("event_time").isNotNull())
    .withWatermark("event_time", "10 seconds")
)

counts = events.groupBy(
    F.window("event_time", "1 minute"), "locality", "severity"
).count()

query = (
    counts.writeStream.outputMode("append")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", "checkpoints/siniestros_bogota")
    .start()
)

query.awaitTermination()

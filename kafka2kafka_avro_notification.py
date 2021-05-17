from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import expr, col, struct, to_json, sum

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Multi Query Demo") \
        .master("yarn") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", 3) \
        .getOrCreate()

    kafka_source_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "invoice-items") \
        .option("startingOffsets", "earliest") \
        .load()

    avroSchema = open('schema/invoice-items', mode='r').read()

    value_df = kafka_source_df.select(from_avro(col("value"), avroSchema).alias("value"))

    rewards_df = value_df.filter("value.CustomerType == 'PRIME'") \
        .groupBy("value.CustomerCardNo") \
        .agg(sum("value.TotalValue").alias("TotalPurchase"),
             sum(expr("value.TotalValue * 0.2").cast("integer")).alias("AggregatedRewards"))

    kafka_target_df = rewards_df.select(expr("value.CustomerCardNo as key"),
                                        to_json(struct("TotalPurchase", "AggregatedRewards")).alias("value"))

    # kafka_target_df.show(truncate=False)

    rewards_writer_query = kafka_target_df \
        .writeStream \
        .queryName("Rewards Writer") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "customer-rewards") \
        .outputMode("update") \
        .option("checkpointLocation", "KafkaAvro/chk-point-dir") \
        .start()

    rewards_writer_query.awaitTermination()

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

#Read from karka-avro source
    kafka_source_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "invoice-items") \
        .option("startingOffsets", "earliest") \
        .load()

#Open avro schema
    avroSchema = open('schema/invoice-items', mode='r').read()

#Deserialization using avro schema and from_avro function
    value_df = kafka_source_df.select(from_avro(col("value"), avroSchema).alias("value"))

#Checking schema
    value_df.printSchema()

#Choosing prime customers and calculate total transactions and earned points
    rewards_df = value_df.filter("value.CustomerType == 'PRIME'") \
        .groupBy("value.CustomerCardNo") \
        .agg(sum("value.TotalValue").alias("TotalPurchase"),
             sum(expr("value.TotalValue * 0.2").cast("integer")).alias("AggregatedRewards"))

#Rename column
    rewards_df = rewards_df.withColumn("CustomerCardNo", expr("`value.CustomerCardNo`")) \
                           .drop("value.CustomerCardNo")

#Serilization to json format
    kafka_target_df = rewards_df.select(expr("CustomerCardNo as key"),
                                        to_json(struct("TotalPurchase", "AggregatedRewards")).alias("value"))

#Check schema
    rewards_df.printSchema()
    # Alternative statement for kafka target
    # kafka_target_df = rewards_df.selectExpr("value.CustomerCardNo as key",
    #                                                                 "to_json(struct(*)) as value")


    # kafka_target_df.show(truncate=False)

#Writestream to kafka topic
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

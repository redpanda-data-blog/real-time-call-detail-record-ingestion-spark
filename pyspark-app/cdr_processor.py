"""
This script reads the CDR records from the `cdr_topic` and performs the aggregation of the call duration
by customer and call date. The result is written to the `cdr_output_topic`.

Usage:
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2 /<directory-path>/cdr_processor.py
"""

# Import the required classes and functions
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define the schema of the CDR records
cdr_schema = StructType(
    [
        StructField("customer_id", StringType()),
        StructField("call_date", StringType()),
        StructField("call_duration", IntegerType()),
        StructField("feedback_score", IntegerType()),
    ]
)

# Create a SparkSession
spark = SparkSession.builder.appName("CDRProcessor").getOrCreate()

# Stream the CDR records from the input topic (cdr_topic)
# The records are read from the Kafka broker running at redpanda-0:9092
# The read stream is stored in cdr_df
cdr_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "redpanda-0:9092")
    .option("subscribe", "cdr_topic")
    .option("startingOffsets", "latest")
    .option("group.id", "cdr_processor_group")
    .load()
)

# Parse the value column of the read stream as JSON and store it in cdr_df
cdr_df = cdr_df.select(from_json(col("value").cast("string"), cdr_schema).alias("cdr"))

# Perform the aggregation
result_df = cdr_df.groupBy("cdr.customer_id", "cdr.call_date").agg(
    sum("cdr.call_duration").alias("total_call_duration")
)

# Write the result to the output topic (cdr_output_topic)
query = (
    result_df.select(to_json(struct(col("*"))).alias("value"))
    .writeStream.outputMode("update")
    .format("kafka")
    .option("kafka.bootstrap.servers", "redpanda-0:9092")
    .option("topic", "cdr_output_topic")
    .option("checkpointLocation", "/pyspark-app/checkpoint-dir")
    .start()
)

# Wait for the query to terminate
query.awaitTermination()

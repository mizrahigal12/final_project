from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, current_timestamp
import os
from pytz import timezone
from datetime import datetime
from project_config import fs

# Connection between Spark and Kafka
os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell"

# Kafka configuration
kafka_bootstrap_servers = "cnt7-naya-cdh63:9092"
kafka_topic = "kafka-crypto-rates"
files_dir = "hdfs://Cnt7-naya-cdh63:8020/user/naya/test/"

# Create SparkSession
spark = SparkSession.builder \
    .appName("KafkaCryptoRates") \
    .getOrCreate()

# Read from Kafka using Spark Structured Streaming
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Extract currency and rate columns from JSON
parsed_df = df.selectExpr("CAST(value AS STRING) as value") \
    .select(explode(from_json(col("value"), "map<string, string>")).alias("currency", "rate")) \
    .withColumn("ts", current_timestamp())

def get_current_datetime_israel():
    israel_tz = timezone('Israel')
    current_datetime = datetime.now(israel_tz)
    return current_datetime

def generate_dynamic_path():
    current_datetime = get_current_datetime_israel()
    year = current_datetime.year
    month = current_datetime.month
    day = current_datetime.day
    hour = current_datetime.hour
    current_dir = f"{files_dir}/{year}/{month}/{day}/{hour}"

    if fs.exists(current_dir):
        print("The directory {} already exists!".format(current_dir))
    else:        
        os.system(f"hdfs dfs -mkdir {current_dir}' ")
    
    return current_dir

# Print the DataFrame
parsed_df.printSchema()

query = parsed_df.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("checkpointLocation", f"{files_dir}/metadata") \
    .foreachBatch(lambda df, epoch_id: df.write.mode("append").json(generate_dynamic_path())) \
    .start()

# Wait for the query to terminate
query.awaitTermination()




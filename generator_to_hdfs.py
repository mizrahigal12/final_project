
import os
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from project_config import fs
from datetime import datetime
from pytz import timezone

# conection between  spark and kafka
os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell"

sc = SparkContext.getOrCreate()
ssc = StreamingContext(sc, batchDuration=5)
spark = SparkSession(sc)

dir_name = 'parquet_files'
stream_directory = "file://" + os.getcwd() + '/' + dir_name + '/'
print(stream_directory)
target_file = r'hdfs://Cnt7-naya-cdh63:8020/user/naya/test'

my_stream = ssc.textFileStream(stream_directory)

schema = StructType([
    StructField("currency", StringType(), nullable=False),
    StructField("rate", StringType(), nullable=False),
    StructField("ts", TimestampType(), nullable=False)
])
# Function to get current datetime in Israel timestamp
def get_current_datetime_israel():
    israel_tz = timezone('Israel')
    current_datetime = datetime.now(israel_tz)
    return current_datetime

def load_to_hdfs(rdd):
    '''
    getting the current datetime and generate the target path
    in format 'path/year/month/day/hour'
    '''
    # Get current timestamp
    current_timestamp = get_current_datetime_israel()
    year = current_timestamp.year
    month = current_timestamp.month
    day = current_timestamp.day
    hour = current_timestamp.hour
    
    # Create directory path based on current timestamp
    directory_path = f"{target_file}/{year}/{month}/{day}/{hour}/"
    
    if fs.exists(directory_path):
        print("The directory {} already exists!".format(directory_path))
    else:        
        os.system(f"hdfs dfs -mkdir {directory_path}' ")

    df = spark.createDataFrame(rdd.map(lambda x: x.split(",")), schema)
    df.write.mode("append").json(directory_path)    

my_stream.foreachRDD(load_to_hdfs)
ssc.start()
ssc.awaitTermination()
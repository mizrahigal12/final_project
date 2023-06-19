from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, to_timestamp
from pyspark.sql.types import FloatType
from project_config import cnx
from pyspark.sql.utils import AnalysisException
import os
from datetime import datetime, timedelta
from pytz import timezone
import pymysql


# conection between  spark and kafka
os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell"

# Create a SparkSession
spark = SparkSession\
    .builder\
    .appName("JsonLoader")\
    .config("spark.driver.extraClassPath", "/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/spark/jars/mysql-connector.jar") \
    .getOrCreate()

# Set HDFS directory path
hdfs_directory = r'hdfs://Cnt7-naya-cdh63:8020/user/naya/test'

#################################################################
# Define functions:
#################################################################
def get_max_timestamp(cnx):
    """
    Connects to a MySQL server, executes a query to select the maximum timestamp from a specific table and column,
    and returns the formatted timestamp as a string object.
	
    Returns:
    - max_timestamp (str): The maximum timestamp as a formatted string in the format 'YYYY-MM-DD HH:00:00'.
    """

    # Create a cursor to execute queries
    cursor = cnx.cursor()

    # Execute the query to select the max timestamp
    query = "SELECT DATE_FORMAT(insert_timestamp, '%Y-%m-%d %H:00:00') FROM process_log order by 1 desc limit 1"

    try:
        with cnx.cursor() as cursor:
            cursor.execute(query)
            max_timestamp = cursor.fetchone()[0]
        cnx.commit()
    except pymysql.Error as e:
        print(f"Error selecting data: {e}")
    # Close the cursor and the connection

    # Return the max timestamp
    return max_timestamp



def insert_to_log_table(timestamp, cnx):
    """
    Inserts a timestamp object into a MySQL table named 'process_log' using the provided connection.
    Args:
        timestamp (datetime.datetime): The timestamp object to be inserted into the table.
        cnx: The MySQL connection object.
    Returns:
        None
    """
    insert_query = "INSERT INTO process_log (insert_timestamp) VALUES (%s)"

    try:
        with cnx.cursor() as cursor:
            cursor.execute(insert_query, (timestamp,))
        cnx.commit()
        print("Data inserted successfully!")
    except pymysql.Error as e:
        print(f"Error inserting data: {e}")

# Function to get current datetime in Israel timestamp
def get_current_datetime_israel():
    israel_tz = timezone('Israel')
    current_datetime = datetime.now(israel_tz)

    # Create a new datetime object with the desired format
    formatted_datetime = datetime(
        year=current_datetime.year,
        month=current_datetime.month,
        day=current_datetime.day,
        hour=current_datetime.hour,
        minute=0,
        second=0
    )

    return formatted_datetime
#################################################################

#################################################################
## Read files from HDFS to MySQL
#################################################################
last_run = datetime.strptime(get_max_timestamp(cnx), '%Y-%m-%d %H:%M:%S')  
end_datetime = get_current_datetime_israel()

while last_run < end_datetime:
    try:
        print('The current last_run:', last_run)

        # Split current datetime into components
        hdfs_current_directory = f"{hdfs_directory}/{last_run.year}/{last_run.month}/{last_run.day}/{last_run.hour}/"
        print('Current directory ---> ', hdfs_current_directory)
        # Load Parquet files into a DataFrame
        df = spark.read.json(f'{hdfs_current_directory}')

        # Convert rate column to float
        df = df.withColumn("rate", col("rate").cast(FloatType()))\
                .withColumn("ts", to_timestamp(col("ts")))

        # Calculate average, minimum, and maximum rate for each currency
        agg_df = df.groupBy("currency").agg(
            avg(col("rate")).alias("average_rate"),
            min(col("rate")).alias("min_rate"),
            max(col("rate")).alias("max_rate")
        )

        #print('The max timestamp is:', str(get_max_timestamp(cnx)))
        # Write aggregated DataFrame to MySQL table
        print(f'collect all the files from {hdfs_current_directory}')
        agg_df.printSchema()
        agg_df.show()
        agg_df.write \
            .format("jdbc") \
            .option("url", 'jdbc:mysql://cnt7-naya-cdh63:3306/crypto_coins_agg') \
            .option("driver", "com.mysql.jdbc.Driver") \
            .option("dbtable", "crypto_coins_data") \
            .option("user", 'nifi') \
            .option("password", 'NayaPass1!') \
            .mode("append") \
            .save()
        
        last_run += timedelta(hours=1)
        insert_to_log_table(last_run , cnx)
        

    except AnalysisException as e:
        # Handle directory not found exception
        if "Path does not exist" in str(e):
            print(f"Directory {hdfs_current_directory} not found. Moving to the next loop iteration.")
            last_run += timedelta(hours=1)
        else:
            # Reraise any other AnalysisException
            raise e
#################################################################

# Stop the SparkSession
spark.stop()

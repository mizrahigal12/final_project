from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Read the Parquet files
df = spark.read.parquet("hdfs://Cnt7-naya-cdh63:8020/user/naya/test/2023/6/13/19/part-00000-ff315cfb-c30b-4e40-8dc3-db17f1e67ede-c000.snappy.parquet")

# Show the data
df.show()
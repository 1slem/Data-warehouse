from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("Mini Data Warehouse") \
    .config("spark.master", "local") \
    .getOrCreate()

# Set log level to INFO
spark.sparkContext.setLogLevel("INFO")

# Reading CSV files
sales_df = spark.read.option("header", True).csv("data/raw/sales.csv")
customers_df = spark.read.option("header", True).csv("data/raw/customers.csv")

# Save files to the Bronze layer
sales_df.write.mode("overwrite").parquet("output/bronze/sales")
customers_df.write.mode("overwrite").parquet("output/bronze/customers")

from pyspark.sql import SparkSession
import datetime
import os

# Create a Spark session
spark = SparkSession.builder \
    .appName("Mini Data Warehouse") \
    .config("spark.master", "local") \
    .getOrCreate()

# Set log level to INFO
spark.sparkContext.setLogLevel("INFO")

# Define ingestion date for folder organization
ingestion_date = datetime.datetime.now().strftime("%Y-%m-%d")

# Define paths
base_path = "output/bronze"
data_sources = {
    "sales": "data/raw/sales.csv",
    "customers": "data/raw/customers.csv",
    "suppliers": "data/raw/suppliers.csv",
    "products": "data/raw/products.csv"
}

# Ensure directory structure exists
os.makedirs(base_path, exist_ok=True)

# Process and save each data source
for source_name, file_path in data_sources.items():
    try:
        # Read raw CSV file
        df = spark.read.option("header", True).csv(file_path)
        
        # Define destination path by source and date
        destination_path = os.path.join(base_path, source_name, ingestion_date)
        
        # Save raw data to the Bronze layer in Parquet format
        df.write.mode("overwrite").parquet(destination_path)
        print(f"{source_name.capitalize()} data successfully stored in Bronze layer.")

    except Exception as e:
        print(f"Error processing {source_name}: {e}")

# Stop Spark session
spark.stop()
print("Data ingestion completed.")
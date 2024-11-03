from pyspark.sql import SparkSession
import os
from pyspark.sql.types import *
from etl_bronze import sales_df, customers_df

### Step 2: Data cleaning and transformation (Silver layer)
# Rename duplicated columns
sales_df = sales_df.withColumnRenamed("OrderID0", "OrderID")

# Cast Quantity to Integer and UnitPrice to Double (for numeric operations)
sales_df = sales_df.withColumn("Quantity", sales_df["Quantity"].cast(IntegerType()))
sales_df = sales_df.withColumn("UnitPrice", sales_df["UnitPrice"].cast(DoubleType()))


sales_clean_df = sales_df.select("OrderID", "CustomerID", "ProductID", "Quantity", "UnitPrice")
customers_clean_df = customers_df.select("CustomerID", "ContactName", "Phone")
sales_df.show()
# Remove empty rows
sales_clean_df = sales_clean_df.na.drop()
customers_clean_df = customers_clean_df.na.drop()
sales_clean_df.show()
# Save the cleaned data to the Silver layer
sales_clean_df.write.mode("overwrite").parquet("output/silver/sales_clean")
customers_clean_df.write.mode("overwrite").parquet("output/silver/customers_clean")
from pyspark.sql import SparkSession
from pyspark.sql.types import *


jar_path = "C:/Program Files/sqljdbc_4.2/fra/jre8/sqljdbc42.jar"
spark = SparkSession.builder \
    .appName("Mini Data Warehouse") \
    .config("spark.driver.extraClassPath", jar_path) \
    .getOrCreate()


### Step 3: Create enriched tables (Gold layer)
# Read the cleaned data from the Silver layer
sales_clean_df = spark.read.parquet("output/silver/sales_clean")
customers_clean_df = spark.read.parquet("output/silver/customers_clean")
# Join sales and customer data
enriched_df = sales_clean_df.join(customers_clean_df, "CustomerID")

# Save the enriched table to the Gold layer
enriched_df.write.mode("overwrite").parquet("output/gold/enriched_sales")



# Step 4: Create a temporary view to query the data
enriched_df.createOrReplaceTempView("enriched_sales")
# SQL query to calculate total revenue per product
result = spark.sql(""" 
    SELECT ProductID, SUM(Quantity * UnitPrice) as TotalRevenue 
    FROM enriched_sales 
    GROUP BY ProductID 
""")
result.show()



# SQL query to calculate total purchases per customer
result_c = spark.sql(""" 
    SELECT CustomerID, SUM(Quantity * UnitPrice) as TotalRevenue 
    FROM enriched_sales 
    GROUP BY CustomerID
""")
result_c.show()



# JDBC Configuration FOR SQL Server
jdbc_url = "jdbc:sqlserver://localhost:1433;databaseName=Sales1;integratedSecurity=true"
jdbc_properties = {
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# LOAD the table in sql server database
sales_clean_df.write.jdbc(url=jdbc_url, table="sales", mode="overwrite", properties=jdbc_properties)
customers_clean_df.write.jdbc(url=jdbc_url, table="customers", mode="overwrite", properties=jdbc_properties)
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, lit, max, year, lag
from pyspark.sql.window import Window

# Configuration de l'environnement Python pour PySpark
os.environ['PYSPARK_PYTHON'] = 'C:/Users/Admin/AppData/Local/Programs/Python/Python311/python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:/Users/Admin/AppData/Local/Programs/Python/Python311/python.exe'
os.environ['HADOOP_HOME'] = "C:/hadoop-3.4.0"
os.environ['PATH'] += os.pathsep + "C:/hadoop-3.4.0/bin"

# Créer une session Spark
spark = SparkSession.builder \
    .appName("Mini Data Warehouse") \
    .getOrCreate()

# Paths to silver and gold layers
silver_layer_products_path = "output/silver/products_clean"
silver_layer_sales_path = "output/silver/sales_clean"
silver_layer_customers_path = "output/silver/customers_clean"
gold_layer_products_path = "output/gold/products_gold"
gold_layer_customers_path = "output/gold/customers_gold"

# Step 1: Load data from the silver layer
products_clean_df = spark.read.parquet(silver_layer_products_path)
sales_clean_df = spark.read.parquet(silver_layer_sales_path)
customers_clean_df = spark.read.parquet(silver_layer_customers_path)

# Step 2: Add OrderYear column to sales data
sales_clean_df = sales_clean_df.withColumn("OrderYear", year(col("OrderDate")))

# -------------------------------
# Product Logic
# -------------------------------

# Step 3: Calculate Total Orders and Last Sale Year, including OrderYear
sales_summary_products = sales_clean_df.groupBy('ProductID', 'OrderYear').agg(
    count('ProductID').alias('total_orders'),
    max(year(col('OrderDate'))).alias('last_sale_year')  # Last year the product was sold
)

# Step 4: Join with Products and Add Product Status
products_with_status = products_clean_df.join(sales_summary_products, on='ProductID', how='left').withColumn(
    'status_produit',
    when(col('total_orders') > 50, 'Très demandé')
    .when((col('total_orders') >= 20) & (col('total_orders') <= 50), 'Moyennement demandé')
    .otherwise('Peu demandé')
)

# Step 5: Identify Obsolete Products
current_year = 2024  # Example current year
products_with_status = products_with_status.withColumn(
    'is_obsolete',
    when(col('last_sale_year') < current_year - 2, lit(1))  # Mark products with no sales in the last 2 years
    .otherwise(lit(0))
)

# Step 6: Filter Out Obsolete Products
filtered_products = products_with_status.filter(col('is_obsolete') == 0)

# Step 7: Add SCD Type 2 Logic
# Ensure OrderYear is available for SCD logic
window_spec_products = Window.partitionBy('ProductID').orderBy('OrderYear')
products_with_history = filtered_products.withColumn(
    'DateDebut', col('OrderYear')
).withColumn(
    'DateFin', lag('OrderYear', -1).over(window_spec_products)
).withColumn(
    'Actif', when(col('DateFin').isNull(), lit(1)).otherwise(lit(0))
)

# Combine All Columns for Gold Layer
products_gold_df = products_with_history

# Step 8: Write Product Data to Gold Layer
if os.path.exists(gold_layer_products_path):
    # Load existing gold layer data
    gold_layer_products_df = spark.read.parquet(gold_layer_products_path)

    # Ensure column compatibility
    for col_name in products_gold_df.columns:
        if col_name not in gold_layer_products_df.columns:
            gold_layer_products_df = gold_layer_products_df.withColumn(col_name, lit(None).cast(products_gold_df.schema[col_name].dataType))
    for col_name in gold_layer_products_df.columns:
        if col_name not in products_gold_df.columns:
            products_gold_df = products_gold_df.withColumn(col_name, lit(None).cast(gold_layer_products_df.schema[col_name].dataType))

    # Align column order
    gold_layer_products_df = gold_layer_products_df.select(products_gold_df.columns)

    # Merge new and existing data
    merged_gold_products_df = products_gold_df.unionByName(gold_layer_products_df).distinct()

    # Overwrite the gold layer with the updated data
    merged_gold_products_df.write.parquet(gold_layer_products_path, mode='overwrite')
else:
    # Write the gold layer for the first time
    products_gold_df.write.parquet(gold_layer_products_path, mode='overwrite')

print("Gold layer for products updated.")

# -------------------------------
# Customer Logic
# -------------------------------

# Step 9: Calculate Total Orders for Customers
sales_summary_customers = sales_clean_df.groupBy('CustomerID', 'OrderYear').agg(
    count('CustomerID').alias('total_orders')
)

# Step 10: Join with Customers and Add Customer Value Segment
customers_with_status = customers_clean_df.join(sales_summary_customers, on='CustomerID', how='left').withColumn(
    'valeur_segment',
    when(col('total_orders') > 10, 'HAUT')
    .when((col('total_orders') >= 5) & (col('total_orders') <= 10), 'MOYEN')
    .otherwise('BAS')
)

# Step 11: Add SCD Type 2 Logic for Customers
window_spec_customers = Window.partitionBy('CustomerID').orderBy('OrderYear')
customers_with_history = customers_with_status.withColumn(
    'DateDebut', col('OrderYear')
).withColumn(
    'DateFin', lag('OrderYear', -1).over(window_spec_customers)
).withColumn(
    'Actif', when(col('DateFin').isNull(), lit(1)).otherwise(lit(0))
)

# Combine All Columns for Gold Layer
customers_gold_df = customers_with_history

# Step 12: Write Customer Data to Gold Layer
if os.path.exists(gold_layer_customers_path):
    # Load existing gold layer data
    gold_layer_customers_df = spark.read.parquet(gold_layer_customers_path)

    # Ensure column compatibility
    for col_name in customers_gold_df.columns:
        if col_name not in gold_layer_customers_df.columns:
            gold_layer_customers_df = gold_layer_customers_df.withColumn(col_name, lit(None).cast(customers_gold_df.schema[col_name].dataType))
    for col_name in gold_layer_customers_df.columns:
        if col_name not in customers_gold_df.columns:
            customers_gold_df = customers_gold_df.withColumn(col_name, lit(None).cast(gold_layer_customers_df.schema[col_name].dataType))

    # Align column order
    gold_layer_customers_df = gold_layer_customers_df.select(customers_gold_df.columns)

    # Merge new and existing data
    merged_gold_customers_df = customers_gold_df.unionByName(gold_layer_customers_df).distinct()

    # Overwrite the gold layer with the updated data
    merged_gold_customers_df.write.parquet(gold_layer_customers_path, mode='overwrite')
else:
    # Write the gold layer for the first time
    customers_gold_df.write.parquet(gold_layer_customers_path, mode='overwrite')

print("Gold layer for customers updated.")

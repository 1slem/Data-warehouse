import logging
import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, IntegerType, FloatType

# Set Python environment for PySpark
os.environ['PYSPARK_PYTHON'] = 'C:/Users/Admin/AppData/Local/Programs/Python/Python311/python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:/Users/Admin/AppData/Local/Programs/Python/Python311/python.exe'
os.environ['HADOOP_HOME'] = "C:/hadoop-3.4.0"
os.environ['PATH'] += os.pathsep + "C:/hadoop-3.4.0/bin"
jar_path = "C:/Program Files/sqljdbc_4.2/fra/jre8/sqljdbc42.jar"

# Configuration de la journalisation
log_file = "logs/products_sales_anomalies.log"
os.makedirs(os.path.dirname(log_file), exist_ok=True)

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Starting product sales and stock analysis.")

# Initialiser SparkSession
spark = SparkSession.builder.appName("Product Sales Analysis").getOrCreate()

# Lecture des DataFrames à partir des fichiers Parquet
sales_clean_df = spark.read.parquet("output/silver/sales_clean")
products_clean_df = spark.read.parquet("output/silver/products_clean")
customers_clean_df = spark.read.parquet("output/silver/customers_clean")

# Fonction pour détecter les anomalies dans les DataFrames
def log_anomalies(df, df_name):
    logging.info(f"Checking for null, missing, and negative values in {df_name}")
    for col_name in df.columns:
        # Vérification des valeurs nulles
        null_count = df.filter(F.col(col_name).isNull()).count()
        if null_count > 0:
            logging.error(f"{df_name} - Column '{col_name}': {null_count} null values detected.")
        
        # Vérification des valeurs négatives (pour les colonnes numériques)
        if isinstance(df.schema[col_name].dataType, (DoubleType, IntegerType, FloatType)):
            negative_count = df.filter(F.col(col_name) < 0).count()
            if negative_count > 0:
                logging.error(f"{df_name} - Column '{col_name}': {negative_count} negative values detected.")
        
        # Vérification des valeurs manquantes (NaN pour les colonnes numériques)
        if isinstance(df.schema[col_name].dataType, (DoubleType, IntegerType, FloatType)):
            missing_count = df.filter(F.isnan(F.col(col_name))).count()
            if missing_count > 0:
                logging.error(f"{df_name} - Column '{col_name}': {missing_count} missing (NaN) values detected.")
        
        # Vérification des stocks négatifs spécifiquement pour les produits
        if df_name == "products_clean_df" and col_name == "UnitsInStock":
            negative_stock_count = df.filter(F.col("UnitsInStock") < 0).count()
            if negative_stock_count > 0:
                logging.error(f"{df_name} - Column 'UnitsInStock': {negative_stock_count} products with negative stock detected.")

# Fonction pour analyser les produits actifs sans ventes depuis un an
def analyze_active_products_without_sales(sales_clean_df, products_clean_df):
    logging.info("Analyzing active products without sales in the last year.")
    
    # Filtrer les produits actifs (non discontinués)
    active_products_df = products_clean_df.filter(F.col("Discontinued") == 0)
    
    # Jointure sur ProductID pour lier les ventes et les produits
    joined_df = sales_clean_df.join(active_products_df, on="ProductID", how="inner")
    
    # Calcul de la date de la dernière vente par produit
    last_sales_date_df = joined_df.groupBy("ProductID").agg(
        F.max("OrderDate").alias("LastSaleDate")
    )
    
    # Calcul de la différence entre la date actuelle et la dernière vente
    current_date_df = last_sales_date_df.withColumn(
        "DaysSinceLastSale", F.datediff(F.current_date(), F.col("LastSaleDate"))
    )
    
    # Filtrer les produits actifs sans ventes depuis plus de 365 jours
    inactive_products_df = current_date_df.filter(F.col("DaysSinceLastSale") > 365)
    
    return inactive_products_df

# Fonction pour analyser les ventes et les stocks des produits
def analyze_product_sales(sales_clean_df, products_clean_df):
    logging.info("Joining sales_clean_df and products_clean_df to analyze product sales and stock.")
    
    # Jointure sur ProductID
    joined_df = sales_clean_df.join(products_clean_df, on="ProductID", how="inner")
    
    # Calcul du total des ventes par produit
    sales_summary_df = joined_df.groupBy("ProductID").agg(
        F.sum("Quantity").alias("TotalSold"),
        F.first("UnitsInStock").alias("StockRemaining"),
        F.first("Discontinued").alias("IsDiscontinued")
    )
    
    # Produits non actifs : ventes nulles ou produits discontinués
    non_active_products_df = sales_summary_df.filter(
        (F.col("TotalSold") == 0) | (F.col("IsDiscontinued") == 1)
    )
    
    # Produits les plus vendus avec leur stock restant
    most_sold_products_df = sales_summary_df.orderBy(F.desc("TotalSold")).select(
        "ProductID", "TotalSold", "StockRemaining"
    )
    
    return non_active_products_df, most_sold_products_df

# Vérification des anomalies dans les trois DataFrames
log_anomalies(sales_clean_df, "sales_clean_df")
log_anomalies(products_clean_df, "products_clean_df")
log_anomalies(customers_clean_df, "customers_clean_df")

# Analyser les produits actifs sans ventes depuis un an
inactive_products_df = analyze_active_products_without_sales(sales_clean_df, products_clean_df)

# Sauvegarder les résultats dans le fichier de log
logging.info("Inactive products without sales in the last year (detailed):")
for row in inactive_products_df.collect():
    logging.info(row.asDict())

# Analyser les ventes et stocks des produits
non_active_products_df, most_sold_products_df = analyze_product_sales(sales_clean_df, products_clean_df)

# Sauvegarder les résultats dans le fichier de log
logging.info("Non-active products (detailed):")
for row in non_active_products_df.collect():
    logging.info(row.asDict())

logging.info("Most sold products with remaining stock (detailed):")
for row in most_sold_products_df.collect():
    logging.info(row.asDict())

# Terminer le processus avec un log
logging.info("Product sales and stock analysis completed.")

# Indiquer où les logs sont sauvegardés
print(f"Les erreurs ont été enregistrées dans : {log_file}")

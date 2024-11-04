from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType, DateType
import logging
import os
import pandas as pd


# Configure the logger to record errors and anomalies
log_file = "logs/logging_config.log"
os.makedirs(os.path.dirname(log_file), exist_ok=True)

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Starting data audit process.")

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Raw Data Audit") \
    .getOrCreate()

# Define base path and date
base_path = "output/bronze"
ingestion_date = "2024-11-04"  # Adjust this if necessary

# Helper function to load DataFrame from Parquet if it exists
def load_parquet_if_exists(path):
    if os.path.exists(path):
        return spark.read.parquet(path)
    else:
        logging.error(f"Path does not exist: {path}")
        return None

# Load data from Bronze layer
sales_df = load_parquet_if_exists(f"{base_path}/sales/{ingestion_date}/")
customers_df = load_parquet_if_exists(f"{base_path}/customers/{ingestion_date}/")   
products_df = load_parquet_if_exists(f"{base_path}/products/{ingestion_date}/")
suppliers_df = load_parquet_if_exists(f"{base_path}/suppliers/{ingestion_date}/")

if not all([sales_df, customers_df, products_df]):
    logging.error("One or more datasets could not be loaded. Stopping audit.")
    spark.stop()
    raise FileNotFoundError("Required datasets not found.")

# Function to audit DataFrames
def audit_dataframe(df, df_name, unique_columns=None):
    try:
        # Check for missing values
        logging.info(f"Auditing missing values for {df_name}")
        missing_values = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])
        missing_values_log = missing_values.collect()
        missing_count = sum([row[c] for row in missing_values_log for c in row.asDict() if row[c] > 0])
        if missing_count > 0:
            logging.error(f"{missing_count} missing values found in {df_name}")
        else:
            logging.info(f"No missing values found in {df_name}")

        def audit_duplicates(df, df_name):
            logging.info(f"Auditing duplicates for {df_name}")

            # Vérification des doublons pour chaque colonne
            for column in df.columns:
                duplicate_count = df.duplicated(subset=[column]).sum()
                if duplicate_count > 0:
                    logging.error(f"{duplicate_count} duplicates found in column '{column}' of {df_name}")
                else:
                    logging.info(f"No duplicates found in column '{column}' of {df_name}")

        # Chargement des fichiers CSV
        customers = pd.read_csv('data/raw/customers.csv')
        products = pd.read_csv('data/raw/products.csv')
        sales = pd.read_csv('data/raw/sales.csv')
        suppliers = pd.read_csv('data/raw/suppliers.csv')

        # Audit des tables
        audit_duplicates(customers, "customers.csv")
        audit_duplicates(products, "products.csv")
        audit_duplicates(sales, "sales.csv")
        audit_duplicates(suppliers, "suppliers.csv")


        # Check date types
        if "OrderDate" in df.columns:
            logging.info(f"Checking date types for {df_name}")
            invalid_dates = df.filter(F.col("OrderDate").cast(DateType()).isNull() & F.col("OrderDate").isNotNull())
            invalid_date_count = invalid_dates.count()
            if invalid_date_count > 0:
                logging.error(f"{invalid_date_count} invalid dates found in {df_name} for 'OrderDate'")

        # Check for valid fax values
        # 4. Vérification des valeurs de Fax
        if "Fax" in df.columns:
            logging.info(f"Vérification des valeurs de Fax pour {df_name}")
            invalid_fax = df.filter(
                (F.col("Fax").isNotNull()) &  # Exclure les valeurs nulles
                (F.col("Fax") != "") &  # Exclure les valeurs vides
                (~F.col("Fax").rlike(r"^[0-9\s\(\)\-\+]+$"))  # Accepter les chiffres, espaces, parenthèses, tirets, et le signe "+"
            )
            invalid_fax_count = invalid_fax.count()
            if invalid_fax_count > 0:
                logging.error(f"{invalid_fax_count} valeurs de Fax invalides trouvées dans {df_name}")
            else:
                logging.info(f"Aucune valeur de Fax invalide trouvée dans {df_name}")


        # Check for logical UnitPrice and Quantity values
        if "UnitPrice" in df.columns:
            logging.info(f"Checking UnitPrice values in {df_name}")
            negative_unit_price_count = df.filter(F.col("UnitPrice") < 0).count()
            invalid_unit_price_count = df.filter(~F.col("UnitPrice").cast(FloatType()).isNotNull()).count()
            if negative_unit_price_count > 0:
                logging.error(f"{negative_unit_price_count} negative UnitPrice values found in {df_name}")
            if invalid_unit_price_count > 0:
                logging.error(f"{invalid_unit_price_count} non-numeric UnitPrice values found in {df_name}")

        if "Quantity" in df.columns:
            logging.info(f"Checking Quantity values in {df_name}")
            non_positive_quantity_count = df.filter(F.col("Quantity") <= 0).count()
            invalid_quantity_count = df.filter(~F.col("Quantity").cast(IntegerType()).isNotNull()).count()
            if non_positive_quantity_count > 0:
                logging.error(f"{non_positive_quantity_count} non-positive Quantity values found in {df_name}")
            if invalid_quantity_count > 0:
                logging.error(f"{invalid_quantity_count} non-numeric Quantity values found in {df_name}")

        # Foreign key validation (e.g., ProductID in sales must exist in products)
        if df_name == "sales" and "ProductID" in df.columns:
            logging.info(f"Validating 'ProductID' foreign key in {df_name}")
            invalid_products = df.join(products_df, "ProductID", "left_anti")
            invalid_product_count = invalid_products.count()
            if invalid_product_count > 0:
                logging.error(f"{invalid_product_count} 'ProductID' in {df_name} do not exist in 'products'")

    except Exception as e:
        logging.error(f"Error during audit of {df_name}: {str(e)}")
        raise e

# Run audits on each DataFrame
audit_dataframe(sales_df, "sales", unique_columns=["OrderID0", "ProductID", "Quantity"])
audit_dataframe(customers_df, "customers", unique_columns=["CustomerID"])
audit_dataframe(products_df, "products", unique_columns=["ProductID"])

logging.info("Data audit completed.")

# Close Spark session
spark.stop()
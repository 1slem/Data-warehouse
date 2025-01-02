import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Set Python environment for PySpark
os.environ['PYSPARK_PYTHON'] = 'C:/Users/Admin/AppData/Local/Programs/Python/Python311/python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:/Users/Admin/AppData/Local/Programs/Python/Python311/python.exe'
os.environ['HADOOP_HOME'] = "C:/hadoop-3.4.0"
os.environ['PATH'] += os.pathsep + "C:/hadoop-3.4.0/bin"

# Initialisation de la session Spark
spark = SparkSession.builder.appName("CurrencyErrorLogging").getOrCreate()

# Configuration du logging
log_file = "logs/currency_anomalies.log"
os.makedirs(os.path.dirname(log_file), exist_ok=True)

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Starting data audit process.")

# Exemple de lecture de DataFrame (assurez-vous que currency_clean_df est déjà chargé)
# currency_clean_df = spark.read.parquet("path_to_your_parquet_file")
# Forcer la conversion des colonnes nécessitant un DoubleType en type float
currency_clean_df = spark.read.parquet("D:/MY_SPARK_PROJECT/output/silver/currency_clean")

# Remplacer 'Converted_final' par 'Converted_Amount'
columns_to_convert = ["Exchange_rate", "montant_final", "Converted_Amount"]

for column in columns_to_convert:
    currency_clean_df = currency_clean_df.withColumn(column, col(column).cast("float"))

# Vérifications et journalisation détaillée
def log_currency_errors_spark(df):
    # Vérification des doublons
    logging.info("Checking for duplicates in each column...")
    for col_name in df.columns:
        duplicated_rows = df.groupBy(col_name).count().filter("count > 1")
        duplicated_count = duplicated_rows.count()
        if duplicated_count > 0:
            logging.warning(f"Column '{col_name}': {duplicated_count} duplicate values detected.")
            # Log each duplicate value found
            duplicate_values = duplicated_rows.collect()
            for row in duplicate_values:
                logging.warning(f"Duplicate value '{row[col_name]}' found in column '{col_name}'.")
        else:
            logging.info(f"Column '{col_name}': No duplicates found.")
    
    # Vérification des valeurs nulles
    logging.info("Checking for null values in each column...")
    for col_name in df.columns:
        null_count = df.filter(col(col_name).isNull()).count()
        if null_count > 0:
            logging.error(f"Column '{col_name}': {null_count} null values detected.")
            # Log null rows found
            null_rows = df.filter(col(col_name).isNull()).collect()
            for row in null_rows:
                logging.error(f"Null value found in column '{col_name}' for row: {row}")
        else:
            logging.info(f"Column '{col_name}': No null values found.")
    
    # Vérification des anomalies spécifiques
    logging.info("Checking for specific data anomalies...")
    
    # 1. Exchange_rate <= 0
    invalid_exchange_rate = df.filter((col("Exchange_rate") <= 0) | col("Exchange_rate").isNull()).count()
    if invalid_exchange_rate > 0:
        logging.error(f"Column 'Exchange_rate': {invalid_exchange_rate} invalid values (<= 0 or null) detected.")
        # Log rows with invalid exchange rates
        invalid_exchange_rows = df.filter((col("Exchange_rate") <= 0) | col("Exchange_rate").isNull()).collect()
        for row in invalid_exchange_rows:
            logging.error(f"Invalid Exchange_rate found: {row['Exchange_rate']} for row: {row}")
    
    # 2. montant_final < 0
    negative_amounts = df.filter(col("montant_final") < 0).count()
    if negative_amounts > 0:
        logging.error(f"Column 'montant_final': {negative_amounts} negative values detected.")
        # Log rows with negative amounts
        negative_amount_rows = df.filter(col("montant_final") < 0).collect()
        for row in negative_amount_rows:
            logging.error(f"Negative montant_final found: {row['montant_final']} for row: {row}")
    
    # 3. Converted_final null
    invalid_converted = df.filter(col("Converted_Amount").isNull()).count()
    if invalid_converted > 0:
        logging.error(f"Column 'Converted_Amount': {invalid_converted} null values detected.")
        # Log rows with null   Converted_Amount
        null_converted_rows = df.filter(col("Converted_Amount").isNull()).collect()
        for row in null_converted_rows:
            logging.error(f"Null Converted_Amount found for row: {row}")
    
    logging.info("Data audit process completed.")

# Appliquer la journalisation
log_currency_errors_spark(currency_clean_df)

# Indiquer où les logs sont sauvegardés
print(f"Les erreurs ont été enregistrées dans : {log_file}")

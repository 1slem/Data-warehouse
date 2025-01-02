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
log_file = "logs/customers_anomalies.log"
os.makedirs(os.path.dirname(log_file), exist_ok=True)

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Starting customer data anomaly detection.")

# Initialiser SparkSession
spark = SparkSession.builder.appName("Customer Data Analysis").getOrCreate()

# Lecture du DataFrame à partir du fichier Parquet
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
        
        # Vérification des adresses manquantes
        if col_name == "Address":
            missing_address_count = df.filter(F.col("Address").isNull() | (F.col("Address") == "")).count()
            if missing_address_count > 0:
                logging.error(f"{df_name} - Column 'Address': {missing_address_count} customers with missing or empty addresses detected.")

# Vérification des anomalies dans customers_clean_df
log_anomalies(customers_clean_df, "customers_clean_df")

# Terminer le processus avec un log
logging.info("Customer data anomaly detection completed.")

# Indiquer où les logs sont sauvegardés
print(f"Les erreurs ont été enregistrées dans : {log_file}")

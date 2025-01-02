# Crée une session Spark
from pyspark.sql import SparkSession
import datetime
import os
import logging

# Initialiser la session Spark
spark = SparkSession.builder \
    .appName("Mini Data Warehouse - Bronze Layer") \
    .getOrCreate()

# Définir la date d'ingestion pour organiser les dossiers
ingestion_date = datetime.datetime.now().strftime("%Y-%m-%d")

# Définir le chemin pour les logs
log_dir = os.path.join("logs", "data_ingestion", ingestion_date)
log_path = os.path.join(log_dir, "data_ingestion.log")

# S'assurer que le dossier de logs existe
os.makedirs(log_dir, exist_ok=True)

# Configurer la journalisation
logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def log_message(message, level="info"):
    """Enregistre les messages de log avec le niveau spécifié."""
    if level == "info":
        logging.info(message)
    elif level == "error":
        logging.error(message)
    print(message)

# Définir les sources de données et les chemins
data_sources = {
    "sales": "data/raw/sales.csv",
    "customers": "data/raw/customers.csv",
    "suppliers": "data/raw/suppliers.csv",
    "products": "data/raw/products.csv"
}

# Base de chemin pour les données de la couche Bronze
bronze_base_path = "output/bronze"  # Remplace par le chemin réel si nécessaire

# Traiter et enregistrer chaque source de données
for source_name, file_path in data_sources.items():
    try:
        # Lire le fichier CSV brut
        df = spark.read.option("header", True).csv(file_path)
        
        # Nommer le DataFrame pour chaque source spécifiquement
        locals()[f"{source_name}_df"] = df
        
        # Définir le chemin de destination par source et date
        bronze_path = os.path.join(bronze_base_path, source_name, ingestion_date)
        
        # S'assurer que le chemin existe
        os.makedirs(bronze_path, exist_ok=True)
        
        # Enregistrer les données dans la couche Bronze au format Parquet
        df.write.mode("overwrite").parquet(bronze_path)
        
        # Log de la réussite de l'ingestion
        log_message(f"Données de {source_name} enregistrées avec succès dans la couche Bronze.")
    
    except Exception as e:
        # Log en cas d'erreur pendant l'ingestion
        log_message(f"Erreur lors du traitement de {source_name}: {e}", level="error")

# Fermer la session Spark
spark.stop()
print("Ingestion des données terminée.")
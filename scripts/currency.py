import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Set Python environment for PySpark
os.environ['PYSPARK_PYTHON'] = 'C:/Users/Admin/AppData/Local/Programs/Python/Python311/python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:/Users/Admin/AppData/Local/Programs/Python/Python311/python.exe'
os.environ['HADOOP_HOME'] = "C:/hadoop-3.4.0"
os.environ['PATH'] += os.pathsep + "C:/hadoop-3.4.0/bin"
jar_path = "C:/Program Files/sqljdbc_4.2/fra/jre8/sqljdbc42.jar"

# Créer une session Spark
spark = SparkSession.builder.appName("CurrencyConversion").getOrCreate()

# Spécifier le chemin vers les fichiers Parquet
silver_path = "output/silver"
sales_clean_df = spark.read.parquet(os.path.join(silver_path, 'sales_clean'))
suppliers_clean_df = spark.read.parquet(os.path.join(silver_path, 'suppliers_clean'))
product_clean_df = spark.read.parquet(os.path.join(silver_path, 'products_clean'))

def generate_currency_df(exchange_rates_file, countries_and_currencies_file):
    # Vérifier que la colonne 'montant_final' existe dans sales_clean_df
    if 'montant_final' not in sales_clean_df.columns:
        raise ValueError("La colonne 'montant_final' est manquante dans 'sales_clean_df'.")
    
    # Charger les taux de change à partir du fichier CSV
    exchange_rates = spark.read.csv(exchange_rates_file, header=True, inferSchema=True)
    
    # Vérifier que les colonnes attendues existent dans le fichier des taux de change
    if not {'Currency', 'Rate'}.issubset(exchange_rates.columns):
        raise ValueError("Le fichier des taux de change doit contenir les colonnes 'Currency' et 'Rate'.")
    
    # Renommer la colonne "Rate" en "Exchange_rate"
    exchange_rates = exchange_rates.withColumnRenamed('Rate', 'Exchange_rate')

    # Charger le fichier CSV des pays et devises
    countries_and_currencies_df = spark.read.csv(countries_and_currencies_file, header=True, inferSchema=True)

    # Joindre sales_clean_df avec product_clean_df sur 'ProductID'
    sales_with_product_df = sales_clean_df.join(
        product_clean_df, on='ProductID', how='inner'
    )

    # Joindre le résultat avec suppliers_clean_df sur 'SupplierID'
    merged_df = sales_with_product_df.join(
        suppliers_clean_df, on='SupplierID', how='inner'
    )

    # Sélectionner les colonnes pertinentes, y compris 'montant_final' et la colonne 'Country' du fournisseur
    merged_df = merged_df.select('OrderDate', 'Country', 'montant_final')

    # Joindre avec le fichier CSV des pays et devises pour obtenir la devise correspondante
    merged_df = merged_df.join(
        countries_and_currencies_df, on='Country', how='left'
    )

    # Sélectionner la colonne 'Currency' comme 'From Currency'
    merged_df = merged_df.withColumnRenamed('Currency', 'From Currency')

    # Ajouter la colonne "To Currency" avec la valeur fixe "EUR"
    merged_df = merged_df.withColumn('To Currency', F.lit('EUR'))

    # Joindre avec les taux de change pour obtenir l'Exchange_rate
    currency_df = merged_df.join(
        exchange_rates.withColumnRenamed('Currency', 'From Currency'),
        on='From Currency',
        how='left'
    )

    # Si la devise de départ est l'Euro, ajuster le taux de change à 1
    currency_df = currency_df.withColumn(
        'Exchange_rate', 
        F.when(currency_df['From Currency'] == 'EUR', F.lit(1))
        .otherwise(currency_df['Exchange_rate'])
    )

    # Sélectionner les colonnes pertinentes pour la conversion
    currency_df = currency_df.select('OrderDate', 'From Currency', 'To Currency', 'Exchange_rate', 'montant_final')

    # Éliminer les lignes où 'From Currency' est 'Unknown'
    currency_df = currency_df.filter(currency_df['From Currency'] != 'Unknown')

    # Ajouter la colonne 'Converted_Amount' (conversion)
    currency_df = currency_df.withColumn(
        'Converted_Amount', 
        F.col('montant_final') * F.col('Exchange_rate')
    )

    # Créer le dossier 'currency_clean' sous output/silver si nécessaire
    currency_output_dir = os.path.join(silver_path, 'currency_clean')
    os.makedirs(currency_output_dir, exist_ok=True)  # S'assurer que le dossier existe

    # Sauvegarder le DataFrame au format Parquet dans le dossier 'currency_clean'
    currency_df.write.mode("overwrite").parquet(currency_output_dir)

    return currency_df

if __name__ == "__main__":
    # Spécifier le fichier des taux de change et le fichier des pays et devises
    exchange_rates_file = "data/raw/currency_exchange_rates.csv"  # Remplacez par le bon chemin du fichier CSV des taux de change
    countries_and_currencies_file = "data/raw/countries_and_currencies.csv"  # Remplacez par le bon chemin du fichier CSV des pays et devises
    
    # Appeler la fonction pour générer le DataFrame des devises et le sauvegarder
    currency_df = generate_currency_df(exchange_rates_file, countries_and_currencies_file)
    
    # Optionnel : Afficher un échantillon des résultats
    currency_df.show()

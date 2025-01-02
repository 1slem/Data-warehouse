import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, StringType
from pyspark.sql.functions import date_format, expr, coalesce, when, year, lit, col, count, upper, trim, regexp_replace, abs, round
import pycountry_convert as pc
from pyspark.sql.functions import udf, to_date
from pyspark.sql import functions as F
import pyspark

# Set Python environment for PySpark
os.environ['PYSPARK_PYTHON'] = 'C:/Users/Admin/AppData/Local/Programs/Python/Python311/python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:/Users/Admin/AppData/Local/Programs/Python/Python311/python.exe'
os.environ['HADOOP_HOME'] = "C:/hadoop-3.4.0"
os.environ['PATH'] += os.pathsep + "C:/hadoop-3.4.0/bin"


# Create a Spark session
spark = SparkSession.builder \
    .appName("DataCleaningAndNormalization") \
    .config("spark.python.worker.reuse", "true") \
    .config("spark.python.worker.memory", "1g") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .config("spark.network.timeout", "120s") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
    .getOrCreate()

# Paths to data
bronze_base_path = "output/bronze"
sales_path = os.path.join(bronze_base_path, "sales")
ingestion_date = max(os.listdir(sales_path))
# Load the data files
tax_data = spark.read.csv("data/raw/Tax_Values_Data_with_ID.csv", header=True, inferSchema=True)
data_currency_df = spark.read.csv("data/raw/Data_currency_with_ID.csv", header=True, inferSchema=True)
customers_clean_df = spark.read.parquet(os.path.join(bronze_base_path, "customers", ingestion_date))
sales_clean_df = spark.read.parquet(os.path.join(bronze_base_path, "sales", ingestion_date))
suppliers_clean_df = spark.read.parquet(os.path.join(bronze_base_path, "suppliers", ingestion_date))
products_clean_df = spark.read.parquet(os.path.join(bronze_base_path, "products", ingestion_date))

# Définir les fonctions de nettoyage et de transformation
    # Renommer les colonnes dupliquées
sales_df = sales_clean_df.withColumnRenamed("OrderID0", "OrderID")

sales_clean_df = sales_clean_df.dropDuplicates()
customers_clean_df = customers_clean_df.dropDuplicates()
suppliers_clean_df = suppliers_clean_df.dropDuplicates()
products_clean_df = products_clean_df.dropDuplicates()

    #Convertir les types de données pour les colonnes quantitatives
sales_clean_df = sales_clean_df.withColumn("Quantity", sales_clean_df["Quantity"].cast(IntegerType()))
sales_clean_df = sales_clean_df.withColumn("UnitPrice", sales_clean_df["UnitPrice"].cast(DoubleType()))
    
    # Nettoyer les colonnes 'Fax' et 'Phone' en supprimant les lettres et en ne gardant que les chiffres
def clean_phone_fax(df, col_name):
    return df.withColumn(col_name, regexp_replace(col(col_name), r"[^0-9]", ""))

    #convertir le type de UnitPrice et les convertir  au format '1k' en '1000'
def convert_price_column(df, column_name="UnitPrice"):
    df = df.withColumn(column_name, regexp_replace(col(column_name), r'[Aa]', ""))
    df = df.withColumn(column_name, regexp_replace(col(column_name), r'k', '000'))
    return df.withColumn(column_name, abs(col(column_name).cast(DoubleType())))

    #Assurer que les valeurs de la colonne quantity sont positives
def ensure_positive_quantity(df, column_name="Quantity"):
    return df.withColumn(column_name, abs(col(column_name)))

#Application du fonction clean
customers_clean_df = clean_phone_fax(customers_clean_df, "Fax")
customers_clean_df = clean_phone_fax(customers_clean_df, "Phone")
suppliers_clean_df = clean_phone_fax(suppliers_clean_df, "Fax")
suppliers_clean_df = clean_phone_fax(suppliers_clean_df, "Phone")

customers_clean_df = customers_clean_df.withColumn("Country", regexp_replace(col("Country"), r"[^a-zA-Z\s]", ""))
sales_clean_df = convert_price_column(sales_clean_df, "UnitPrice")
products_clean_df = convert_price_column(products_clean_df, "UnitPrice")
sales_clean_df = ensure_positive_quantity(sales_clean_df, "Quantity")

def replace_empty_with_default(df):
    """
    Remplace les colonnes vides ou nulles par des valeurs par défaut.
    - "NULL" pour les colonnes de type String
    - 0 pour les colonnes de type numérique

    Args:
        df (DataFrame): DataFrame à nettoyer.

    Returns:
        DataFrame: DataFrame nettoyé avec les valeurs par défaut.
    """
    for column, data_type in df.dtypes:
        if data_type == 'string':
            # Remplace les valeurs vides ou nulles par "NULL" pour les colonnes de type string
            df = df.withColumn(column, when((col(column).isNull()) | (col(column) == ""), "NULL").otherwise(col(column)))
        elif data_type in ['int', 'bigint', 'double', 'float']:
            # Remplace les valeurs nulles par 0 pour les colonnes numériques
            df = df.withColumn(column, when(col(column).isNull(), 0).otherwise(col(column)))
    return df
#appel au fct
customers_clean_df = replace_empty_with_default(customers_clean_df)
sales_clean_df = replace_empty_with_default(sales_clean_df)
suppliers_clean_df = replace_empty_with_default(suppliers_clean_df)
products_clean_df = replace_empty_with_default(products_clean_df)

# Rename orderID0 colomn to OrderID
sales_clean_df = sales_clean_df.withColumnRenamed("OrderId0", "OrderId")
# Drop the column 'OrderId14'
sales_clean_df = sales_clean_df.drop("OrderID14")

# Ajouter des colonnes pour les statuts
sales_summary = sales_clean_df.groupBy('CustomerID').agg(count('CustomerID').alias('total_orders'))
customers_with_status = customers_clean_df.join(sales_summary, on='CustomerID', how='left')
customers_with_status = customers_with_status.withColumn(
    'status_client',
    when(col('total_orders') > 10, 'VIP')
    .when((col('total_orders') >= 5) & (col('total_orders') <= 10), 'Fidèle')
    .otherwise('Occasionnel')
)
customers_clean_df = customers_with_status.drop('total_orders')
# Ajouter des colonnes pour les produits
sales_summary_products = sales_clean_df.groupBy('ProductID').agg(count('ProductID').alias('total_orders'))
products_with_status = products_clean_df.join(sales_summary_products, on='ProductID', how='left')
products_with_status = products_with_status.withColumn(
    'status_produit',
    when(col('total_orders') > 50, 'Très demandé')
    .when((col('total_orders') >= 20) & (col('total_orders') <= 50), 'Moyennement demandé')
    .otherwise('Peu demandé')
)
products_clean_df = products_with_status.drop('total_orders')
# Nettoyer la colonne ShipPostalCode : enlever les caractères non numériques
sales_clean_df = sales_clean_df.withColumn('ShipPostalCode', regexp_replace(col('ShipPostalCode'), r'[^0-9]', ''))
sales_clean_df = sales_clean_df.filter(col('ShipPostalCode').rlike('^[0-9]+$'))
sales_clean_df = sales_clean_df.withColumn('ShipPostalCode', col('ShipPostalCode').cast(IntegerType()))

# Nettoyer la colonne ShipPostalCode : enlever les caractères non numériques
sales_clean_df = sales_clean_df.withColumn('ShipPostalCode', regexp_replace(col('ShipPostalCode'), r'[^0-9]', ''))
sales_clean_df = sales_clean_df.filter(col('ShipPostalCode').rlike('^[0-9]+$'))
sales_clean_df = sales_clean_df.withColumn('ShipPostalCode', col('ShipPostalCode').cast(IntegerType()))

# Fonction pour remplacer les lettres accentuées
def replace_accents(df, column_name):
    accents = [
        ('à|á|â|ä|ã|å|æ', 'a'),
        ('è|é|ê|ë', 'e'),
        ('ì|í|î|ï', 'i'),
        ('ò|ó|ô|ö|õ|ø', 'o'),
        ('ù|ú|û|ü', 'u'),
        ('ç', 'c'),
        ('ñ', 'n'),
        ('ÿ', 'y')
    ]
    for pattern, replacement in accents:
        df = df.withColumn(column_name, regexp_replace(col(column_name), pattern, replacement))
    return df

sales_clean_df = replace_accents(sales_clean_df, 'ShipCountry')
sales_clean_df = replace_accents(sales_clean_df, 'ShipCity')
customers_clean_df = replace_accents(customers_clean_df, 'City')
customers_clean_df = replace_accents(customers_clean_df, 'Country')

# Nettoyer les caractères spéciaux restants
sales_clean_df = sales_clean_df.withColumn('ShipCountry', regexp_replace(col('ShipCountry'), r'[^\w\s]', ''))
sales_clean_df = sales_clean_df.withColumn('ShipCity', regexp_replace(col('ShipCity'), r'[^\w\s]', ''))


# Convert the 'country' column to lowercase
customers_lowercase_df = customers_clean_df.withColumn('country', col('country'))

# Dictionary to handle country name variations and corrections
country_name_corrections = {
    "UK": "United Kingdom",
    "Germani": "Germany",
    "Poretugal": "Portugal",
}

# Apply country corrections using the dictionary
for incorrect, correct in country_name_corrections.items():
    customers_lowercase_df = customers_lowercase_df.withColumn(
        'country', 
        when(col('country') == incorrect, correct).otherwise(col('country'))
    )

# Function to get the continent code from the country name
def get_continent_from_country(country):
    try:
        # Convert country name to alpha-2 code (e.g., "United States" -> "US")
        country_code = pc.country_name_to_country_alpha2(country, cn_name_format="default")
        # Get the continent code (e.g., "US" -> "NA")
        continent_code = pc.country_alpha2_to_continent_code(country_code)
        return continent_code
    except Exception:
        return "UNKNOWN"  # Return "UNKNOWN" if the country is not found

# Register the function as a UDF
get_continent_udf = udf(get_continent_from_country, StringType())

# Apply the UDF to create the 'code_region' column
customers_clean_df = customers_lowercase_df.withColumn(
    'code_region', get_continent_udf(col('country'))
)

# Verify the result
customers_clean_df.select('country', 'code_region').show()

# Save the updated DataFrame with 'code_region' column to the silver layer
silver_base_path = "output/silver"
customers_clean_df.write.mode("overwrite").parquet(
    os.path.join(silver_base_path, "customers_clean", ingestion_date)
)


# Define the columns to process and update data
date_columns = ["OrderDate", "RequiredDate", "ShippedDate"]
for column in date_columns:
    sales_clean_df = sales_clean_df.withColumn(
        column,
        when(
            col(column).isNotNull(),
            expr(f"""
                CASE
                    WHEN year(to_date({column}, 'MM/dd/yy')) = 1996 THEN to_date({column}, 'MM/dd/yy') + interval 26 years
                    WHEN year(to_date({column}, 'MM/dd/yy')) = 1997 THEN to_date({column}, 'MM/dd/yy') + interval 26 years
                    WHEN year(to_date({column}, 'MM/dd/yy')) = 1998 THEN to_date({column}, 'MM/dd/yy') + interval 26 years
                    ELSE to_date({column}, 'MM/dd/yy')
                END
            """)
        ).otherwise(None)  # Handle null or invalid dates
    )

# Debugging: Show a sample of the transformed data and save transformation
sales_clean_df.select(*date_columns).show(10, truncate=False)
sales_clean_df.write.mode("overwrite").parquet(os.path.join(silver_base_path, "sales_clean"))
print("Date updated and saved successfully.")

# Join sales data with tax values
tax_data = tax_data.withColumn("Country", upper(trim(col("Country"))))
sales_clean_df = sales_clean_df.withColumn("ShipCountry", upper(trim(col("ShipCountry"))))
spark.conf.set("spark.sql.caseSensitive", "false")

# Join sales data with tax values
country_name_mapping = {
    "USA": "UNITED STATES",
    "UK": "UNITED KINGDOM",
}
# Apply the standardization function to sales_clean_df and tax_data
def standardize_country_names(df, column_name, mapping):
    for incorrect, correct in mapping.items():
        df = df.withColumn(column_name, when(col(column_name) == incorrect, correct).otherwise(col(column_name)))
    return df

# Apply the standardization function
sales_clean_df = standardize_country_names(sales_clean_df, "ShipCountry", country_name_mapping)
tax_data = standardize_country_names(tax_data, "Country", country_name_mapping)

# Join sales_clean_df with tax_data based on country and year
sales_with_tax = sales_clean_df.join(
    tax_data,
    (sales_clean_df["ShipCountry"] == tax_data["Country"]) &
    (year(sales_clean_df["OrderDate"]) == tax_data["Year"]),
    how="left"
)

#Verify that tax_data has entries for all combinations of Country and Year that exist in sales_clean_df
sales_clean_df.select("ShipCountry", year("OrderDate").alias("Year")).distinct().exceptAll(
    tax_data.select("Country", "Year").distinct()
).drop()

# Replace missing tax values with 0 and rename the column for clarity
sales_with_tax = sales_with_tax.withColumn(
    "tax_value", coalesce(col("Tax_Percentage"), lit(0))
)

# Drop unnecessary columns
sales_with_tax = sales_with_tax.drop("Country", "Year", "Tax_Percentage")

# Calculate the FinalAmount column with tax applied
sales_with_tax = sales_with_tax.withColumn(
    "FinalAmount",
    round((col("UnitPrice") * col("Quantity")) * (1 - col("Discount")) * (1 + col("tax_value") ), 2)
)

sales_with_tax.select("ShipCountry", "OrderDate", "tax_value").show()
print("Sales data with tax calculations updated and saved successfully.")

sales_clean_df = sales_with_tax


# Currency Part
def generate_currency_df(data_currency_df, sales_clean_df, customers_clean_df, silver_base_path):
    if not isinstance(data_currency_df, pyspark.sql.dataframe.DataFrame):
        raise ValueError("Expected a PySpark DataFrame for data_currency_df")
    
    # Validate the presence of 'FinalAmount' in sales_clean_df
    if 'FinalAmount' not in sales_clean_df.columns:
        raise ValueError("The column 'FinalAmount' is missing in 'sales_clean_df'.")
    
    # Validate required columns in data_currency_df
    required_columns = ['country', 'currency', 'exchange_rate_to_euro', 'Valid_From', 'Valid_To']
    for column in required_columns:
        if column not in data_currency_df.columns:
            raise ValueError(f"The file 'Data_currency.csv' must contain the columns {required_columns}.")

    # Standardize column names and data
    data_currency_df = (
        data_currency_df
        .withColumnRenamed('exchange_rate_to_euro', 'Exchange_rate')
        .withColumn('Valid_From', to_date(col('Valid_From'), 'yyyy-MM-dd'))
        .withColumn('Valid_To', to_date(col('Valid_To'), 'yyyy-MM-dd'))
        .withColumn('country', F.upper(F.trim(col('country'))))
    )
    sales_clean_df = sales_clean_df.withColumn('ShipCountry', F.upper(F.trim(col('ShipCountry'))))
    customers_clean_df = customers_clean_df.withColumn('Country', F.upper(F.trim(col('Country'))))

    # Join sales_clean_df with customers_clean_df to get country information
    sales_with_country_df = sales_clean_df.join(
        customers_clean_df.select("CustomerID", "Country"),
        on="CustomerID",
        how="left"
    )

    # Join sales_with_country_df with data_currency_df for exchange rates
    merged_df = sales_with_country_df.join(
        data_currency_df,
        sales_with_country_df['ShipCountry'] == data_currency_df['country'],
        how="left"
    )

    # Validate date range for exchange rates
    merged_df = merged_df.withColumn(
        'Valid_Rate',
        (col('OrderDate') >= col('Valid_From')) & (col('OrderDate') <= col('Valid_To'))
    ).filter(col('Valid_Rate') == True)

    # Default exchange rate for EUR if missing
    merged_df = merged_df.withColumn(
        'Exchange_rate',
        when(col('currency') == 'EUR', lit(1)).otherwise(col('Exchange_rate'))
    )

    # Create the 'Converted_Amount' column
    merged_df = merged_df.withColumn(
        'Converted_Amount',
        round((col('FinalAmount') * col('Exchange_rate')), 2)
    )

    # Select relevant columns for output
    currency_df = merged_df.select(
        'CurrencyID', 'OrderDate', 'ShipCountry', 'currency', 'Exchange_rate', 'FinalAmount', 'Converted_Amount'
    )

    # Save the currency_clean DataFrame
    currency_output_dir = os.path.join(silver_base_path, 'currency_clean')
    os.makedirs(currency_output_dir, exist_ok=True)
    currency_df.write.mode("overwrite").parquet(currency_output_dir)

    return currency_df

# Ensure column standardization before processing
data_currency_df = data_currency_df.withColumn("country", F.upper(F.trim(col("country"))))
sales_clean_df = sales_clean_df.withColumn("ShipCountry", F.upper(F.trim(col("ShipCountry"))))
# Generate the final currency DataFrame
sales_with_currency_df = generate_currency_df(
    data_currency_df, sales_clean_df, customers_clean_df, silver_base_path
)
sales_with_currency_df.show()

# Join `sales_clean_df` with `sales_with_currency_df` to add the CurrencyID to sales_clean_df
sales_clean_with_currency = sales_clean_df.join(
    sales_with_currency_df.select("OrderDate", "ShipCountry", "CurrencyID"),
    on=["OrderDate", "ShipCountry"],
    how="left"
)
# Replace NULL values in the CurrencyID column with -1
sales_clean_with_currency = sales_clean_with_currency.fillna({'CurrencyID': -1})

# Print completion message
print("CurrencyID successfully added to sales DataFrame and saved.")

# Calender Part

from pyspark.sql.functions import monotonically_increasing_id, col, dayofmonth, month, year

# Add CalendarID to the sales DataFrame
# Generate a unique CalendarID for each row using monotonically_increasing_id()
sales_with_calendar_id = sales_clean_with_currency.withColumn("CalendarID", monotonically_increasing_id())

# Debugging step: Verify that CalendarID was added successfully
sales_with_calendar_id.select("OrderDate", "CalendarID").show(5)

# Create a new DataFrame for the calendar information
# Select the CalendarID and OrderDate columns from sales DataFrame
calendar_df = sales_with_calendar_id.select(
    col("CalendarID"),
    col("OrderDate").alias("Date")
)

# Extract Day, Month, and Year from the Date column
calendar_df = calendar_df.withColumn("Day", dayofmonth(col("Date"))) \
                         .withColumn("Month", month(col("Date"))) \
                         .withColumn("Year", year(col("Date")))

# Debugging step: Verify the new columns in calendar_df
calendar_df.show(5)

# Save the calendar DataFrame as 'calendar_clean' in Parquet format
silver_base_path = "output/silver"
calendar_output_path = os.path.join(silver_base_path, "calendar_clean")

# Remove existing files in the output directory to avoid any conflicts
if os.path.exists(calendar_output_path):
    shutil.rmtree(calendar_output_path)

# Save the calendar DataFrame
calendar_df.write.mode('overwrite').parquet(calendar_output_path)

# Step 5: Save the updated sales DataFrame with CalendarID
sales_output_path = os.path.join(silver_base_path, "sales_clean")
sales_with_calendar_id.write.mode('overwrite').parquet(sales_output_path)

# Save other cleaned DataFrames
sales_with_calendar_id.write.parquet(os.path.join(silver_base_path, "sales_clean"), mode='overwrite')
customers_clean_df.write.parquet(os.path.join(silver_base_path, "customers_clean"), mode='overwrite')
suppliers_clean_df.write.parquet(os.path.join(silver_base_path, "suppliers_clean"), mode='overwrite')
products_clean_df.write.parquet(os.path.join(silver_base_path, "products_clean"), mode='overwrite')
sales_with_currency_df.write.parquet(os.path.join(silver_base_path, "currency_clean"), mode='overwrite')
tax_data.write.parquet(os.path.join(silver_base_path, "tax_clean"), mode='overwrite')

## Save cleaned DataFrames in Load layer 
gold_base_path = "output/gold"
calendar_df.write.parquet(os.path.join(gold_base_path, "calendar_clean"), mode='overwrite')
sales_with_calendar_id.write.parquet(os.path.join(gold_base_path, "sales_clean"), mode='overwrite')
suppliers_clean_df.write.parquet(os.path.join(gold_base_path, "suppliers_clean"), mode='overwrite')
sales_with_currency_df.write.parquet(os.path.join(gold_base_path, "currency_clean"), mode='overwrite')
tax_data.write.parquet(os.path.join(gold_base_path, "tax_clean"), mode='overwrite')

# Stop the Spark session
spark.stop()
print("Data transformation completed successfully.")

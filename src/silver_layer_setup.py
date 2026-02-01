import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# SparkSession with Iceberg 3.4 support
spark = SparkSession.builder \
    .appName("CryptoBronzeToSilver") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "/opt/airflow/data/warehouse") \
    .getOrCreate()

print("--- Spark Session Started ---")

try:
    # 1. Ingestion - Enabling MULTILINE to handle formatted JSON records
    bronze_path = "/opt/airflow/data/bronze/*.json"
    print(f"Starting read process from: {bronze_path}")
    
    # 'multiLine=True' resolves _corrupt_record errors if the JSON is pretty-printed
    df = spark.read.option("multiLine", "true").json(bronze_path)
    
    # Check if the dataframe contains valid data instead of just corruption errors
    if "_corrupt_record" in df.columns and len(df.columns) == 1:
        print("!!! ERROR: JSON content is unreadable for Spark!")
    else:
        row_count = df.count()
        print(f"Successfully read: {row_count} rows.")

        # 2. Transformation
        silver_df = df.withColumn("processed_at", current_timestamp())

        # 3. Namespace and Table persistence
        spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")

        # Atomic replace/create table
        silver_df.write \
            .format("iceberg") \
            .mode("overwrite") \
            .saveAsTable("local.db.crypto_prices")

        print("--- SUCCESS: Data persisted in Iceberg table! ---")

except Exception as e:
    print(f"!!! AN ERROR OCCURRED !!!: {e}")

finally:
    spark.stop()
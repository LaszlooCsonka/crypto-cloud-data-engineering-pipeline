from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
import sys

if __name__ == "__main__":
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("CryptoToBigQueryPrep") \
        .getOrCreate()

    # Configuration: GCS Paths
    input_path = "gs://crypto-data-pipeline-2026-storage/raw_data/*.json"
    output_path = "gs://crypto-data-pipeline-2026-storage/processed_data/"
    
    print(f"--- SCANNING FILES: {input_path} ---")

    try:
        # 1. Ingestion: Using multiline option for Coingecko JSON format
        raw_df = spark.read.option("multiLine", "true").json(input_path)
        
        print("--- RAW DATA SCHEMA: ---")
        raw_df.printSchema()

        # 2. Transformation: Extracting Bitcoin and Ethereum data
        # Coingecko JSON structure: {"bitcoin": {"usd": ...}, "ethereum": {"usd": ...}}
        btc_df = raw_df.select(
            lit("BTC").alias("symbol"),
            col("bitcoin.usd").cast("double").alias("price")
        )

        eth_df = raw_df.select(
            lit("ETH").alias("symbol"),
            col("ethereum.usd").cast("double").alias("price")
        )

        # Union tables and add processing timestamp
        final_df = btc_df.union(eth_df).withColumn("processed_at", current_timestamp())

        # 3. Persistence: Saving in Parquet format (Silver Layer)
        final_df.write.mode("overwrite").parquet(output_path)
        
        print("--- TRANSFORMATION AND SAVING SUCCESSFUL ---")
        final_df.show()

    except Exception as e:
        print(f"!!! CRITICAL ERROR DURING SPARK EXECUTION: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()
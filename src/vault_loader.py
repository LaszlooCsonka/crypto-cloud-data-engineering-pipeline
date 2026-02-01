import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("CryptoDataVaultLoad") \
    .getOrCreate()

# Steps 1-4: Ingestion and Vault Preparation
raw_df = spark.read.option("multiLine", "true").json("/opt/airflow/data/bronze/*.json")

for col_name in raw_df.columns:
    if col_name != '_corrupt_record':
        raw_df = raw_df.withColumn(col_name, F.struct(F.col(f"{col_name}.usd").cast("double").alias("usd")))

stack_str = ", ".join([f"'{c}', {c}" for c in raw_df.columns if c != '_corrupt_record'])
exploded_df = raw_df.selectExpr(f"stack({len([c for c in raw_df.columns if c != '_corrupt_record'])}, {stack_str}) as (symbol, details)")

final_df = exploded_df.select(
    F.col("symbol"),
    F.col("details.usd").alias("price_usd"),
    F.current_timestamp().alias("load_date"),
    F.lit("API_COINGECKO").alias("record_source")
).withColumn("h_crypto_hash_key", F.sha2(F.upper(F.col("symbol")), 256))

# 5. HUB Persistence (Silver Layer)
hub_path = "/opt/airflow/data/silver/vault/h_cryptocurrency"
hub_df = final_df.select("h_crypto_hash_key", "symbol", "record_source").distinct()
hub_df.write.mode("overwrite").parquet(hub_path)

# 6. SATELLITE Persistence (Silver Layer - Historical Tracking)
sat_path = "/opt/airflow/data/silver/vault/s_crypto_prices"
sat_df = final_df.select("h_crypto_hash_key", "price_usd", "load_date", "record_source")
sat_df.write.mode("append").parquet(sat_path)

# 7. GOLD Layer: Latest Prices Snapshot
# Re-reading the full Satellite to include historical records for windowing
all_hubs = spark.read.parquet(hub_path)
all_sats = spark.read.parquet(sat_path)

windowSpec = Window.partitionBy("symbol").orderBy(F.col("load_date").desc())

gold_df = all_hubs.join(all_sats, "h_crypto_hash_key") \
    .withColumn("row_number", F.row_number().over(windowSpec)) \
    .filter(F.col("row_number") == 1) \
    .select("symbol", "price_usd", "load_date")

gold_path = "/opt/airflow/data/gold/latest_prices"
gold_df.write.mode("overwrite").parquet(gold_path)

print(f"Process completed successfully! Gold layer updated at: {gold_path}")
spark.stop()
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, expr
from pyspark.sql.types import DecimalType

def create_spark_session():
    return SparkSession.builder \
        .appName("FinancialReconciliation") \
        .getOrCreate()

def process_orders(spark, input_path):
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    # Cast amount to Decimal(10,2) and ensure timestamp is properly formatted
    df_processed = df.withColumn("amount", col("amount").cast(DecimalType(10, 2))) \
                     .withColumn("timestamp_utc", to_timestamp("timestamp_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    return df_processed

def process_settlements(spark, input_path):
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    # Convert timestamp_est to UTC (Assume EST is UTC-5 for this mock)
    # Cast amounts to Decimal(10,2)
    df_processed = df.withColumn("settled_amount", col("settled_amount").cast(DecimalType(10, 2))) \
                     .withColumn("fee", col("fee").cast(DecimalType(10, 2))) \
                     .withColumn("timestamp_utc", expr("to_timestamp(timestamp_est, 'yyyy-MM-dd''T''HH:mm:ss') + interval 5 hours")) \
                     .drop("timestamp_est")
    return df_processed

def reconcile_and_save_fallback(orders_df, settlements_df, fallback_path):
    # Rename timestamps to avoid duplicate column errors on Parquet write
    orders_df = orders_df.withColumnRenamed("timestamp_utc", "order_timestamp")
    settlements_df = settlements_df.withColumnRenamed("timestamp_utc", "settlement_timestamp")
    
    # Perform full outer join to find discrepancies
    joined = orders_df.join(settlements_df, "order_id", "full_outer")
    breaks = joined.filter(
        (col("amount") != col("settled_amount")) |
        col("amount").isNull() |
        col("settled_amount").isNull()
    )
    # Save a small sample of the reconciled discrepancies locally
    breaks.limit(1000).write.mode("overwrite").parquet(fallback_path)

if __name__ == "__main__":
    spark = create_spark_session()
    
    # Use absolute paths for robustness
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    # For a real pipeline, we'd read from s3a://s3-fintech-recon-bronze-ss/
    # For local testing/mocking, we support env vars with local fallbacks
    orders_input = os.environ.get("ORDERS_INPUT_PATH", os.path.join(base_dir, "internal_orders.csv"))
    settlements_input = os.environ.get("SETTLEMENTS_INPUT_PATH", os.path.join(base_dir, "gateway_settlements.csv"))
    
    orders_output = os.environ.get("ORDERS_OUTPUT_PATH", os.path.join(base_dir, "silver_orders.parquet"))
    settlements_output = os.environ.get("SETTLEMENTS_OUTPUT_PATH", os.path.join(base_dir, "silver_settlements.parquet"))
    
    fallback_output = os.path.join(base_dir, "fallback_data.parquet")
    
    orders_df = process_orders(spark, orders_input)
    settlements_df = process_settlements(spark, settlements_input)
    
    # Write to Silver (Mocking S3 with local paths or accepting S3 paths via env)
    orders_df.write.mode("overwrite").parquet(orders_output)
    settlements_df.write.mode("overwrite").parquet(settlements_output)
    
    reconcile_and_save_fallback(orders_df, settlements_df, fallback_output)
    
    print("PySpark processing completed.")
    spark.stop()

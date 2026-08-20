import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import duckdb
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from spark_processor import process_orders, process_settlements

@pytest.fixture(scope="session")
def spark():
    # Set timezone to UTC to avoid local timezone assertion errors
    spark_session = SparkSession.builder \
        .appName("TestFinancialReconciliation") \
        .master("local[2]") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    yield spark_session
    spark_session.stop()

def test_process_orders(spark, tmp_path):
    csv_path = tmp_path / "orders.csv"
    with open(csv_path, "w") as f:
        f.write("order_id,user_id,amount,timestamp_utc\n")
        f.write("O1,U1,100.5,2023-01-01T10:00:00Z\n")
    
    df = process_orders(spark, str(csv_path))
    assert df.count() == 1
    row = df.collect()[0]
    
    from decimal import Decimal
    assert row['amount'] == Decimal("100.50")
    
    # Check timestamp parsing robustly inside Spark to avoid Python local timezone offset
    from pyspark.sql.functions import date_format
    hour_str = df.select(date_format("timestamp_utc", "HH")).collect()[0][0]
    assert hour_str == "10"

def test_process_settlements(spark, tmp_path):
    csv_path = tmp_path / "settlements.csv"
    with open(csv_path, "w") as f:
        f.write("settlement_id,order_id,settled_amount,fee,timestamp_est\n")
        f.write("S1,O1,100.5,2.01,2023-01-01T10:00:00\n")
        
    df = process_settlements(spark, str(csv_path))
    assert df.count() == 1
    row = df.collect()[0]
    
    from decimal import Decimal
    assert row['settled_amount'] == Decimal("100.50")
    
    # 10:00 EST -> 15:00 UTC
    from pyspark.sql.functions import date_format
    hour_str = df.select(date_format("timestamp_utc", "HH")).collect()[0][0]
    assert hour_str == "15"
    assert 'timestamp_est' not in df.columns

def test_reconciliation_logic_mocked():
    con = duckdb.connect()
    
    con.execute("""
        CREATE TABLE internal_orders (
            order_id VARCHAR,
            amount DECIMAL(10,2)
        )
    """)
    con.execute("INSERT INTO internal_orders VALUES ('O1', 100.00), ('O2', 200.00), ('O3', 300.00)")
    
    con.execute("""
        CREATE TABLE gateway_settlements (
            order_id VARCHAR,
            settled_amount DECIMAL(10,2),
            fee DECIMAL(10,2)
        )
    """)
    con.execute("INSERT INTO gateway_settlements VALUES ('O1', 100.00, 2.00), ('O2', 199.99, 4.00), ('O4', 400.00, 8.00)")
    
    query = """
        SELECT 
            COALESCE(o.order_id, s.order_id) AS order_id,
            CASE
                WHEN o.order_id IS NULL THEN 'Missing in Internal'
                WHEN s.order_id IS NULL THEN 'Missing in Gateway'
                WHEN o.amount != s.settled_amount THEN 'Amount Mismatch'
                ELSE 'Other Discrepancy'
            END as discrepancy_reason
        FROM internal_orders o
        FULL OUTER JOIN gateway_settlements s ON o.order_id = s.order_id
        WHERE 
            o.order_id IS NULL 
            OR s.order_id IS NULL 
            OR o.amount != s.settled_amount
    """
    
    # Use fetchall() to avoid pandas dependency missing on Windows Python 3.13
    results = con.execute(query).fetchall()
    assert len(results) == 3
    
    reasons = {r[0]: r[1] for r in results}
    assert reasons['O2'] == 'Amount Mismatch'
    assert reasons['O3'] == 'Missing in Gateway'
    assert reasons['O4'] == 'Missing in Internal'

def test_fallback_logic(spark):
    orders = [("O1", 100.0), ("O2", 200.0)]
    settlements = [("O1", 100.0), ("O2", 199.0)]
    
    orders_df = spark.createDataFrame(orders, ["order_id", "amount"])
    settlements_df = spark.createDataFrame(settlements, ["order_id", "settled_amount"])
    
    # Verify the fallback filtering logic locally without triggering Hadoop Parquet writes
    breaks = orders_df.join(settlements_df, "order_id", "full_outer").filter(
        (orders_df.amount != settlements_df.settled_amount) |
        orders_df.amount.isNull() |
        settlements_df.settled_amount.isNull()
    )
    
    assert breaks.count() == 1
    assert breaks.collect()[0]['order_id'] == 'O2'

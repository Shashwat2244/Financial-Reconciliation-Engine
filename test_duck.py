import duckdb
try:
    res = duckdb.execute("SELECT count(*) FROM read_parquet('fallback_data.parquet/*.parquet')").fetchall()
    print("SUCCESS", res)
except Exception as e:
    print("ERROR", str(e))

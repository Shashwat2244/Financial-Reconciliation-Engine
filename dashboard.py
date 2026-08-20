import streamlit as st
import snowflake.connector
import duckdb
import pandas as pd
import os
from dotenv import load_dotenv

# Ensure absolute paths are used
script_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(script_dir, '.env'))

st.set_page_config(page_title="Financial Reconciliation Dashboard", layout="wide")
st.title("Multi-Source Financial Settlement & Reconciliation")

@st.cache_data(ttl=60)
def get_data():
    """
    Attempts to fetch data from Snowflake. 
    If connection fails, gracefully falls back to local DuckDB querying a Parquet file.
    """
    try:
        # Validate credentials exist first to avoid lengthy timeouts
        user = os.getenv('SNOWFLAKE_USER')
        if not user or user == "your_snowflake_user":
            raise ValueError("Snowflake credentials not configured.")

        conn = snowflake.connector.connect(
            user=user,
            password=os.getenv('SNOWFLAKE_PASSWORD', ''),
            account=os.getenv('SNOWFLAKE_ACCOUNT', ''),
            database='FINANCE_DB',
            schema='RECONCILIATION_SCHEMA',
            warehouse='COMPUTE_WH'
        )
        query = "SELECT * FROM daily_reconciliation_breaks"
        df = pd.read_sql(query, conn)
        df.columns = df.columns.str.lower()
        conn.close()
        source = "Snowflake (Live)"
        return df, source
    except Exception as e:
        st.warning(f"Snowflake connection failed: {e}. Falling back to local DuckDB.")
        
        # Fallback to DuckDB querying the local parquet
        fallback_path = os.path.join(script_dir, "fallback_data.parquet")
        if os.path.exists(fallback_path):
            conn = duckdb.connect()
            # PySpark writes parquets as partitioned directories, so DuckDB needs a glob pattern
            query = f"SELECT * FROM read_parquet('{fallback_path}/*.parquet')"
            df = conn.execute(query).df()
            conn.close()
            source = "DuckDB (Fallback Parquet)"
            
            # Since fallback is raw joined data from Spark, we derive the reason
            if not df.empty and 'amount' in df.columns and 'settled_amount' in df.columns:
                def get_reason(row):
                    if pd.isna(row['amount']): return 'Missing in Internal'
                    if pd.isna(row['settled_amount']): return 'Missing in Gateway'
                    if row['amount'] != row['settled_amount']: return 'Amount Mismatch'
                    return 'Other'
                df['discrepancy_reason'] = df.apply(get_reason, axis=1)
                
            return df, source
        else:
            st.error(f"Fallback data not found at {fallback_path}.")
            return pd.DataFrame(), "None"

df, source = get_data()

st.subheader(f"Data Source: {source}")

if not df.empty:
    st.metric("Total Reconciliation Breaks", len(df))
    
    if 'discrepancy_reason' in df.columns:
        st.write("### Breaks by Reason")
        st.bar_chart(df['discrepancy_reason'].value_counts())
    
    st.write("### Raw Break Data")
    st.dataframe(df)
else:
    st.info("No data available to display.")

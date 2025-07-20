#!/usr/bin/env python
import argparse
import os

import pandas as pd
from sqlalchemy import create_engine, text

# custom ETL script using pandas + SQLAlchemy

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    with engine.begin()  as connection:
        print(f"Dropping table {table_name} if it exists...")
        connection.execute(text(f"DROP TABLE IF EXISTS {table_name};"))
        connection.commit()

    df_iter = pd.read_csv('ecommerce_sales.csv', iterator=True, chunksize=10000, dtype=str, low_memory=False)

    for i, df in enumerate(df_iter):
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_").str.replace("-", "_")

        expected_cols = [
            "category", "size", "date", "status", "fulfilment", "style", "sku", "asin",
            "courier_status", "qty", "amount", "b2b", "currency"
        ]
        df = df[expected_cols].copy()

        df["date"] = pd.to_datetime(df["date"], format="%m-%d-%y", errors="coerce")
        df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0).astype(int)
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
        df["b2b"] = df["b2b"].map({"True": True, "False": False})

        df.to_sql(name=table_name, con=engine, if_exists="append", index=False)

        print(f"Inserted chunk {i+1}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')


    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of table where we will write results to')

    args = parser.parse_args()
    main(args)

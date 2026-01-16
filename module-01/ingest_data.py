#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine


def ingest_csv(engine, path, table_name):
    print(f"Ingesting CSV: {path} → {table_name}")
    df = pd.read_csv(path)

    df.head(0).to_sql(
        name=table_name,
        con=engine,
        if_exists='replace'
    )

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists='append'
    )

    print(f"✅ Done: {table_name} ({len(df)} rows)")


def ingest_parquet(engine, path, table_name):
    print(f"Ingesting Parquet: {path} → {table_name}")
    df = pd.read_parquet(path)

    df.head(0).to_sql(
        name=table_name,
        con=engine,
        if_exists='replace'
    )

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists='append'
    )

    print(f"✅ Done: {table_name} ({len(df)} rows)")


@click.command()
@click.option('--user', required=True)
@click.option('--password', required=True)
@click.option('--host', required=True)
@click.option('--port', required=True, type=int)
@click.option('--db', required=True)
@click.option('--zones-csv', required=True, help='Path to taxi zone CSV')
@click.option('--green-parquet', required=True, help='Path to green tripdata parquet')
def main(user, password, host, port, db, zones_csv, green_parquet):
    """Ingest taxi zone + green tripdata into Postgres"""

    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{db}'
    )

    ingest_csv(
        engine,
        zones_csv,
        table_name='taxi_zone_data'
    )

    ingest_parquet(
        engine,
        green_parquet,
        table_name='green_tripdata_2025_11'
    )


if __name__ == '__main__':
    main()

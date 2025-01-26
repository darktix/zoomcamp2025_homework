#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import pandas as pd
import pyarrow.parquet as pq
from time import time
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    
    file_name = url.split('/')[-1]
    chunk_size = 100000

    # Download file
    os.system(f"wget -O {file_name} {url}")

    # Connect to Postgres
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Handle different file formats with iterator
    if file_name.endswith('.parquet'):
        parquet_file = pq.ParquetFile(file_name)
        # Get first batch
        # first_batch = next(parquet_file.iter_batches(batch_size=chunk_size))
        # df = first_batch.to_pandas()
        
        # Create iterator for remaining batches
        df_iter = (batch.to_pandas() 
                for batch in parquet_file.iter_batches(batch_size=chunk_size))
    elif file_name.endswith('.gz'):
        df_iter = pd.read_csv(file_name, iterator=True, chunksize=chunk_size, compression='gzip')
    elif file_name.endswith('.csv'):
        df_iter = pd.read_csv(file_name, iterator=True, chunksize=chunk_size)
    else:
        raise ValueError('File must be .parquet, .csv or .csv.gz')

    df =next(df_iter)

    # Convert datetime columns to datetime type
    for col in df.columns:
        if 'datetime' in col.lower():
            df[col] = pd.to_datetime(df[col])

    # Create table
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    # Insert first chunk
    df.to_sql(name=table_name, con=engine, if_exists='append')

    # Insert remaining chunks
    while True:
        t_start = time()
        df = next(df_iter)
        for col in df.columns:
            if 'datetime' in col.lower():
                df[col] = pd.to_datetime(df[col])

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()
        print('inserted another chunk..., took %.3f seconds' % (t_end-t_start))


if __name__ == '__main__':
    
    parser = argparse. ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)

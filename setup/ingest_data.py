#!/usr/bin/env python
# coding: utf-8


from sqlalchemy import create_engine
import pandas as pd
from time import time
import argparse
import os


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    parquet_file = 'output.parquet'
    csv_file = 'output.csv'


    os.system(f"wget {url} -O {parquet_file}")

    
    df = pd.read_parquet(parquet_file)

    df.to_csv('output.csv')

    df_iter = pd.read_csv(csv_file, iterator = True, chunksize=100000)

    df = next(df_iter)

    df.drop(labels = 'Unnamed: 0', axis=1)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df.head(n=0).to_sql(name = table_name, con=engine, if_exists='replace')


    while True:
        t_start = time()
        df = next(df_iter)
        df.to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time()
        print('inserted another chuck, took %.3f second' % (t_end - t_start))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
                        prog='Parameters for data pipelines',
                        description='Passes arguments for the pipeline i.e user, password, db name, file path, port')


    parser.add_argument('--user', help = 'user name for postgres')
    parser.add_argument('--password', help = 'password for postgres')
    parser.add_argument('--host', help = 'host for postgres')
    parser.add_argument('--db', help = 'database name for postgres')
    parser.add_argument('--table-name', help = 'name of table for postgres')
    parser.add_argument('--port', help = 'port for postgres')
    parser.add_argument('--url', help = 'url for file')

    args = parser.parse_args()

    main(args)
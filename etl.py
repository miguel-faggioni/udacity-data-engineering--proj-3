#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""ETL pipeline for the Cloud Data Warehouse project for Udacity.

This file loads data into the staging tables of the Redshift cluster,
then inserts the data into the analytics tables for later analysis.

Usage example:
  $ python etl.py

"""

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print('>>> Query successful: {}'.format(query))
        except Exception as e:
            print(e)
            print('>>> Query failed: {}'.format(query))
            return


def insert_tables(cur, conn):
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print('>>> Query successful: {}'.format(query))
        except Exception as e:
            print(e)
            print('>>> Query failed: {}'.format(query))
            return


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()

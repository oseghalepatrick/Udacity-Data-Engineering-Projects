import time
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function execute queries that load data into the staging tables in a redshift database

    Parameters:
    cur (psycopg2.extensions.cursor): The cursor object to execute the query.
    conn (psycopg2.extensions.connection): The connection object to use for the query.

    Returns:
    None
    """
    for query in copy_table_queries:
        st = time.time()
        print(f"Started copying {query.split()[1]} !!!")
        cur.execute(query)
        conn.commit()
        print(f"Successfully copied data into {query.split()[1]} table in {time.time()-st} seconds\n")


def insert_tables(cur, conn):
    """
    This function execute queries that insert data into tables in a redshift database

    Parameters:
    cur (psycopg2.extensions.cursor): The cursor object to execute the query.
    conn (psycopg2.extensions.connection): The connection object to use for the query.

    Returns:
    None
    """
    for query in insert_table_queries:
        st = time.time()
        print(f"Started inserting into {query.split()[2]} !!!")
        cur.execute(query)
        conn.commit()
        print(f"Successfully inserted data into {query.split()[2]} table in {time.time()-st} seconds\n")


def main():
    """
    This function calls the insert into tables functions and create connection to the database.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
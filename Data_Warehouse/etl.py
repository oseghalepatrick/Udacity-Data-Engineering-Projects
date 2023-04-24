import time
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        st = time.time()
        print(f"Started copying {query.split()[1]} !!!")
        cur.execute(query)
        conn.commit()
        print(f"Successfully copied data into {query.split()[1]} table in {time.time()-st} seconds\n")


def insert_tables(cur, conn):
    for query in insert_table_queries:
        st = time.time()
        print(f"Started inserting into {query.split()[2]} !!!")
        cur.execute(query)
        conn.commit()
        print(f"Successfully inserted data into {query.split()[2]} table in {time.time()-st} seconds\n")


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
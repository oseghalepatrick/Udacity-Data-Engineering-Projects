import time
import configparser
import psycopg2
from sql_queries import quality_checks_queries


def quality_checks(cur, conn):
    for query in quality_checks_queries:
        st = time.time()
        print(f"Started copying {query.split()[-1][:-1]} !!!")
        cur.execute(query)
        conn.commit()
        print(f"Successfully copied data into {query.split()} table in {time.time()-st} seconds\n")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    quality_checks(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
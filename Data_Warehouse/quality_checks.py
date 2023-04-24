import time
import configparser
import psycopg2
from sql_queries import quality_checks_queries


def quality_checks(cur, conn):
    """
    This function execute queries that select from tables in a redshift database

    Parameters:
    cur (psycopg2.extensions.cursor): The cursor object to execute the query.
    conn (psycopg2.extensions.connection): The connection object to use for the query.

    Returns:
    None
    """
    for query in quality_checks_queries:
        st = time.time()
        cur.execute(query)
        output = cur.fetchall()
        print(f"{query.split()[-1][:-1]} table has {output[0][0]} records\n")


def main():
    """
    This function call the quality checks function and create connection to the database
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('\n')
    quality_checks(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
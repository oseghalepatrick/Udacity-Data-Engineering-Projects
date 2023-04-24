import re
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        print(f"Successfully dropped {query.split()[4]} table\n")
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        print(f"Successfully created {query.split()[5]} table\n")
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    values = re.findall("=(\w+)", str(conn.dsn))
    connection_string = f"postgresql://{values[0]}:{values[1]}@{values[3]}:{values[4]}/{values[2]}"
    print(connection_string)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
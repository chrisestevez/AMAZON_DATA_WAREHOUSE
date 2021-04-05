import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drops all tables in database.

    Args:
        cur (obj): Cursor
        conn (obj): Connection to database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create all tables for database.

    Args:
        cur (obj): Cursor
        conn (obj): Connection to database
    """    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Executes steps to create all table for database
    """
    print("Creating tables Process Started")
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DB'].values()))
    cur = conn.cursor()

    print("Dropping Tables")
    drop_tables(cur, conn)
    
    print("Creating Tables")
    create_tables(cur, conn)

    conn.close()
    print("Tables Created")


if __name__ == "__main__":
    main()
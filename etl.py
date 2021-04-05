import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load information into staging table

    Args:
        cur (obj): Cursor
        conn (obj): Connection
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Load information into staging table

    Args:
        cur (obj): Cursor
        conn (obj): Connection
    """   
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Executes script logic. 
    """
    print("ETL Process Started")
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DB'].values()))
    cur = conn.cursor()
    
    print("Loding Staging Tables")
    load_staging_tables(cur, conn)
    
    print("Inserting Data")
    insert_tables(cur, conn)

    conn.close()
    print("ETL Process Completed")


if __name__ == "__main__":
    main()
from db_connect import connect, close
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Run queries to load JSON data from S3 into the staging tables."""
    for query in copy_table_queries:
        cur.execute(query)


def insert_tables(cur, conn):
    """Run queries to insert data from staging tables into analytics tables."""
    for query in insert_table_queries:
        cur.execute(query)


def main():
    cur, conn = connect()
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    close(cur, conn)


if __name__ == "__main__":
    main()

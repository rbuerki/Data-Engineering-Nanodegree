from db_connect_local import connect, close
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Run queries for deleting all pre-existing tables to clean the database."""
    for query in drop_table_queries:
        cur.execute(query)


def create_tables(cur, conn):
    """Run queries for creating defined tables (staging, fact, dim) in the database."""
    for query in create_table_queries:
        cur.execute(query)


def main():
    cur, conn = connect()
    drop_tables(cur, conn)
    create_tables(cur, conn)
    close(cur, conn)


if __name__ == "__main__":
    main()

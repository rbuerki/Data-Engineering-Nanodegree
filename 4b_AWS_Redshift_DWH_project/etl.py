from db_connect import connect, close
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)


def main():
    cur, conn = connect()
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    close()


if __name__ == "__main__":
    main()

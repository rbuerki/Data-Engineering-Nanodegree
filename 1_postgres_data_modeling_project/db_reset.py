"""
This file is for development purpose only. If run from the terminal it resets
the database. (--> Drops all existing tables and recreates them empty.)
"""


import psycopg2
from db_credentials import DB_USER, DB_PW
from sql_queries import create_table_queries, drop_table_queries

# Set connection defaults
host = "127.0.0.1"
dbdefault = "postgres"
dbname = "sparkifydb"
user = DB_USER
password = DB_PW


def create_database():
    # Connect to default database
    conn = psycopg2.connect(f"""host={host} dbname={dbdefault}
                                user={user} password={password}""")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # Create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # Close connection to default database
    conn.close()

    # Connect to sparkify database
    conn = psycopg2.connect(f"""host={host} dbname={dbname}
                               user=pos{user}tgres password={password}""")
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()

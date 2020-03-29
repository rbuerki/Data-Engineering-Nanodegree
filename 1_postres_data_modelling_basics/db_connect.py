# The aaproach here has been inspired by:
# https://www.postgresqltutorial.com/postgresql-python/connect/

import psycopg2
from configparser import ConfigParser


def config(filename="database.ini", section="postgresql"):
    # Create a parser to read config file
    parser = ConfigParser()
    parser.read(filename)

    # Get section, default to postgresql
    db_params = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db_params[param[0]] = param[1]
    else:
        raise Exception(f"Section {section} not found in the {filename} file.")

    return db_params


def connect():
    """ Connect to the PostgreSQL database server. Return cursor and connection."""
    conn = None
    try:
        # Read connection parameters
        db_params = config()
        # Connect to the PostgreSQL server
        print("Connecting to the PostgreSQL database...")
        conn = psycopg2.connect(**db_params)
        # Set auto commit so that each action is commited without calling conn.commit()
        conn.set_session(autocommit=True)
        # Create a cursor
        cur = conn.cursor()
        print("Success!")

        return cur, conn

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def close(cur, conn):
    """Close the communication with the PostgreSQL database."""
    try:
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")

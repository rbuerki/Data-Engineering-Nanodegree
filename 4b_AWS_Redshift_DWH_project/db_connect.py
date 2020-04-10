# The aproach here has been inspired by:
# https://www.postgresqltutorial.com/postgresql-python/connect/

import psycopg2
from configparser import ConfigParser


def config(filename="dwh.cfg", section="CLUSTER"):
    """Read and return necessary parameters for connecting to the database."""
    # Create a parser to read config file
    parser = ConfigParser()
    parser.read(filename)

    # Get section, default to CLUSTER
    db_params = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db_params[param[0]] = param[1]
    else:
        raise Exception(f"Section {section} not found in the {filename} file.")

    return db_params


def connect():
    """Connect to the Redshift cluster. Return cursor and connection objects."""
    conn = None
    try:
        # Read connection parameters
        db_params = config()
        print(db_params)
        # Connect to the PostgreSQL server
        print("Connecting to the Redshift cluster ...")
        conn = psycopg2.connect(
            f"host={db_params.get('host')} \
              dbname={db_params.get('db_name')} \
              user={db_params.get('db_user')} \
              password={db_params.get('db_password')} \
              port={db_params.get('db_port')}"
              )
        # Set auto commit so that each action is commited without calling conn.commit()
        conn.set_session(autocommit=True)
        # Create a cursor
        cur = conn.cursor()
        print("Success!")

        return cur, conn

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def close(cur, conn):
    """Close the communication with the database."""
    try:
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")

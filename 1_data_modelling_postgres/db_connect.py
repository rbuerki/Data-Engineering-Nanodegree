import psycopg2
from db_credentials import DB_USER, DB_PW

# Set defaults
host = "127.0.0.1"
dbname = "sparkifydb"
user = DB_USER
password = DB_PW


def get_connection():
    """Connect to database and return connection and cursor objects."""
    conn = psycopg2.connect(f"""host={host}
                                dbname={dbname}
                                user={user}
                                password={password}""")
    # Set auto commit so that each action is commited without calling conn.commit()
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    return conn, cur

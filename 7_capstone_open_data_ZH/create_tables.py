import logging
from db_connect import connect, close
from sql_queries import create_table_queries, drop_table_queries

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


def drop_tables(cur, conn):
    """Run queries for deleting all pre-existing tables to clean the database."""
    for query in drop_table_queries:
        cur.execute(query)


def create_tables(cur, conn):
    """Run queries for creating defined tables (staging, fact, dim) in the database."""
    for query in create_table_queries:
        cur.execute(query)


def main():

    logger.info("Connecting to DB ...")
    cur, conn = connect()
    logger.info("Dropping existing tables ...")
    drop_tables(cur, conn)
    logger.info("Creating tables ...")
    create_tables(cur, conn)
    close(cur, conn)
    logger.info("All done. Database connection closed.")


if __name__ == "__main__":
    main()

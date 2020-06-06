import logging
from db_connect import connect, close
# from s3_access import access_s3
from sql_queries import (
    copy_table_queries,
    insert_table_queries,
    drop_staging_queries
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


def load_staging_tables(cur, conn):
    """Run queries to load JSON data from S3 into the staging tables."""
    for query in copy_table_queries:
        cur.execute(query)


def insert_tables(cur, conn):
    """Run queries to insert data from staging tables into analytics tables."""
    for query in insert_table_queries:
        cur.execute(query)


def drop_staging_tables(cur, conn):
    """Run queries to drop the staging tables."""
    for query in drop_staging_queries:
        cur.execute(query)


def main():
    logger.info("Connecting to DB ...")
    cur, conn = connect()
    logger.info("Loading staging tables ...")
    load_staging_tables(cur, conn)
    logger.info("Inserting anaytics tables ...")
    insert_tables(cur, conn)
    logger.info("Dropping staging tables ...")
    drop_staging_tables(cur, conn)
    close(cur, conn)
    logger.info("All done. Database connection closed.")


if __name__ == "__main__":
    main()

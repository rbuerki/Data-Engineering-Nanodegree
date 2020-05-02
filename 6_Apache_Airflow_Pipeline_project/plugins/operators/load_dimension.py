from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """Load data into dimensional table from staging tables.

    Parameters:
    -----------
    - redshift_conn_id: Conn Id of the Airflow connection to redshift database
    - destination_table: name of the fact table to update
    - sql_statement: 'SELECT' query to retrieve rows for insertion in fact table
    - update_mode (optional): 'insert' or 'overwrite'. 'overwrite' truncates destination
        table before inserting rows. Defaults to 'overwrite'

    Returns:
    --------
    - None

    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 destination_table="",
                 sql_statement="",
                 update_mode="overwrite",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.sql_statement = sql_statement
        self.update_mode = update_mode

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        sql_query = f"INSERT INTO {self.destination_table} {self.sql_statement}"
        if self.update_mode == "overwrite":
            sql_query = f"TRUNCATE {self.destination_table}; {sql_query}"

        self.log.info(f"Loading data into {self.destination} table")
        redshift.run(sql_query)

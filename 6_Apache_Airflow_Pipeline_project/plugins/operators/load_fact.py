from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """Load data into fact table from staging tables.

    Parameters:
    -----------
    - redshift_conn_id: Conn Id of the Airflow connection to redshift database
    - destination_table: name of the fact table to update
    - sql_statement: 'SELECT' query to retrieve rows for insertion in fact table

    Returns:
    --------
    - None

    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 destination_table="",
                 sql_statement="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.destination_table=destination_table
        self.sql_statement=sql_statement

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        sql_query = f"INSERT INTO {self.destination_table} {self.sql_statement}"

        self.log.info('Loading data into fact table')
        redshift.run(sql_query)

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    """Run data quality checks on one or more tables.

    Parameters:
    -----------
    - redshift_conn_id: Conn Id of the Airflow connection to redshift database
    - sql_query_list: A list of one or more queries to check data.
    - table_list: A list of one or more tables for the data check queries.
    - expected_results: A list of expected results for each data check query.

    Returns:
    --------
        - Exception raised on data check failure.
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query_list=None,
                 table_list=None,
                 expected_results=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query_list = sql_query_list
        self.tableList = table_list
        self.expected_results = expected_results

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)

        self.log.info('DataQualityOperator not implemented yet')

        checks = zip(self.data_check_query, self.table, self.expected_result)

        for check in checks:
            try:
                redshift.run(check[0].format(check[1])) == check[2]
                self.log.info('Data quality check passed.')
            except:
                self.log.info('Data quality check failed.')
                raise AssertionError('Data quality check failed.')

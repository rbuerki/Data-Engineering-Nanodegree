from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """Transfer data from S3 to staging tables in redshift database.

    Parameters:
    -----------
    - aws_credentials_id: Conn Id of the Airflow connection to Amazon Web Services
    - redshift_conn_id: Conn Id of the Airflow connection to redshift database
    - table: name of the staging table to populate
    - s3_bucket: name of S3 bucket, e.g. "udacity-dend"
    - s3_key: name of S3 key. This field is templatable when context is enabled
    - json_format (optional): path to JSONpaths file, defaults to "auto"

    Returns: None

    """
    ui_color = '#358140'
    template_fields = ("s3_key")

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_format="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_format = json_format

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # Set S3 path based on rendered key
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        sql_staging = f"""
            COPY {self.table}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'
            TIMEFORMAT as 'epochmillisecs'
            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
            REGION 'us-west-2'
            FORMAT AS JSON {self.json_format};
            """

        self.log.info("Clearing data from destination Redshift table")
        redshift.run(f"DELETE * FROM {self.table}")

        self.log.info("Copying data from S3 to Redshift")
        redshift.run(sql_staging)

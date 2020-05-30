# The approach taken here has been inspired by:
# https://www.postgresqltutorial.com/postgresql-python/connect/

import logging
import boto3
from botocore.exceptions import ClientError, ParamValidationError
from configparser import ConfigParser


logger = logging.getLogger(__name__)
logger.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.DEBUG,
    datefmt="%m/%d/%Y %H:%M:%S"
)


def config(filename="dwh.cfg", section="AWS"):
    """Read and return necessary parameters for connecting to the database."""
    # Create a parser to read config file
    parser = ConfigParser()
    parser.read(filename)

    # Get section, default to AWS
    s3_params = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            s3_params[param[0]] = param[1]
    else:
        raise Exception(f"Section {section} not found in the {filename} file.")

    return s3_params


def access_S3(db_params):
    """Acess to the Redshift cluster. Return bucket."""
    try:
        # Read connection parameters
        s3_params = config()
        logger.debug(s3_params)
        # Connect to the PostgreSQL server
        print("Connecting to S3 ...")
        key = db_params.get('key')
        secret = db_params.get('secret')

        s3 = boto3.resource(
            "s3",
            region_name="eu-west-1",
            aws_access_key_id=key,
            aws_secret_access_key=secret
        )
        s3_bucket = s3.Bucket("raph-dend-zh-data")
        logger.debug([obj for obj in s3_bucket.objects])

    except ParamValidationError as e:
        print("Parameter validation error: %s" % e)
    except ClientError as e:
        print("Unexpected error: %s" % e)

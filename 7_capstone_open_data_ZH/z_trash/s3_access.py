# This file was used for experimentation and debugging only.

import boto3
from botocore.exceptions import ClientError, ParamValidationError
from configparser import ConfigParser


def config_s3(filename="dwh.cfg", section="AWS"):
    """Read and return necessary parameters for connecting to the s3 bucket."""
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


def access_s3():
    """Acess to the Redshift cluster. Return bucket object."""
    try:
        # Read connection parameters
        s3_params = config_s3()
        # Connect to the PostgreSQL server
        key = s3_params.get('key')
        secret = s3_params.get('secret')

        s3 = boto3.resource(
            "s3",
            region_name="eu-west-1",
            aws_access_key_id=key,
            aws_secret_access_key=secret
        )
        s3_bucket = s3.Bucket("raph-dend-zh-data")
        # for obj in s3_bucket.objects.filter(Prefix="data/raw/verkehrszaehlungen/non_mot/"):
        #     print(obj)

    except ParamValidationError as e:
        print("Parameter validation error: %s" % e)
    except ClientError as e:
        print("Unexpected error: %s" % e)

    return s3_bucket

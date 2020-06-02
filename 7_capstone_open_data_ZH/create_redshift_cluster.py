import configparser
import boto3
from botocore.exceptions import ClientError, ParamValidationError


def get_cluster_params(param_file):
    """Extract and return cluster parameters from configuration file.

    Prerequisites: AWS key and secret and an IAM role that allows
    Redshift cluster to call AWS services (in this case s3).

    Parameters
    ----------
    param_file : str
        Filename of the configuration file (.cfg)

    Returns
    -------
    tuple
        Necessary parameters and credentials for cluster creation:
        KEY, SECRET, CLUSTER_TYPE, NUM_NODES, NODE_TYPE,
        CLUSTER_IDENTIFIER, DB_NAME, DB_USER, DB_PASSWORD,
        DWH_PORT, IAM_ROLE_NAME
    """
    config = configparser.ConfigParser()
    config.read_file(open(param_file))

    KEY = config.get("AWS", "KEY")
    SECRET = config.get("AWS", "SECRET")
    CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
    CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
    NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
    NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")
    IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
    DB_NAME = config.get("CLUSTER", "DB_NAME")
    DB_USER = config.get("CLUSTER", "DB_USER")
    DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
    DB_PORT = config.get("CLUSTER", "DB_PORT")

    return(
        KEY,
        SECRET,
        CLUSTER_TYPE,
        NUM_NODES,
        NODE_TYPE,
        CLUSTER_IDENTIFIER,
        DB_NAME,
        DB_USER,
        DB_PASSWORD,
        DB_PORT,
        IAM_ROLE_NAME
    )


def get_roleARN(iam_role_name, key, secret):
    """Retrieve ARN (Amazon Resource Name) of existing IAM role
    to enable S3 read-only access from Redshift.

    Parameters
    ----------
    iam_role_name : str
        Name of existing IAM role with S3 read-only access policy
    key : str
        Access key ID for programmatic access to AWS API
    secret : str
        Secret Access Key for programmatic access to AWS API

    Returns
    -------
    str
        ARN: Amazon Resource Name for IAM role.
    """
    iam = boto3.client(
        "iam",
        region_name="eu-west-1",
        aws_access_key_id=key,
        aws_secret_access_key=secret
    )

    try:
        return iam.get_role(RoleName=iam_role_name)["Role"]["Arn"]
    except ClientError as e:
        print(f"Unexpected error: {e}.")


def create_redshift_cluster(
        key,
        secret,
        cluster_type,
        node_type,
        num_nodes,
        cluster_identifier,
        db_name,
        db_user,
        db_password,
        roleArn
        ):
    """Create AWS redshift cluster via API.

    Parameters
    ----------
    key : str
        AWS API access ID key.
    secret : str
        AWS API secret key.
    cluster_type : str
        Either "single-node" or "multi-node".
    node_type : str
        e.g. "dc2.large".
    num_nodes : str
        Number of nodes forming the cluster.
    cluster_identifier : str
        Cluster name.
    db_name : str
        Database name.
    db_user : str
        [description]
    db_password : str
        [description]
    roleArn : str
        Amazon Resource Name (ARN) for IAM role.

    Returns
    -------
    redshift client
        Redshift DB client.
    """

    redshift = boto3.client(
        "redshift",
        region_name="eu-west-1",
        aws_access_key_id=key,
        aws_secret_access_key=secret
    )

    try:
        redshift.create_cluster(
            ClusterType=cluster_type,
            NodeType=node_type,
            NumberOfNodes=int(num_nodes),
            ClusterIdentifier=cluster_identifier,
            DBName=db_name,
            MasterUsername=db_user,
            MasterUserPassword=db_password,
            IamRoles=[roleArn]
        )
        print("Creating cluster. Check management console for status.")
    except ParamValidationError as e:
        print("Parameter validation error: %s" % e)
    except ClientError as e:
        print(f"Unexpected error: {e}.")

    cluster_properties = redshift.describe_clusters(
        ClusterIdentifier=cluster_identifier
    )['Clusters'][0]
    return cluster_properties


def open_tcp_port(key, secret, port, cluster_properties, IP="0.0.0.0/0"):
    """Open an incoming TCP port to access the cluster endpoint.

    Attention: Using IP 0.0.0.0/0 is very insecure, do not use
    in real world projects.

    Parameters
    ----------
    key : str
        AWS API access ID key.
    secret : str
        AWS API secret key.
    redshift : redshift client
        Redshift DB client.
    IP : str, optional
        IP-Adress, by default 0.0.0.0/0
    """
    ec2 = boto3.resource(
        "ec2",
        region_name="eu-west-1",
        aws_access_key_id=key,
        aws_secret_access_key=secret
    )

    try:
        vpc = ec2.Vpc(id=cluster_properties['VpcId'])
        # Access the default group (last in the list, shaky code ;-))
        defaultSg = list(vpc.security_groups.all())[-1]

        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp=IP,
            IpProtocol='TCP',
            FromPort=int(port),
            ToPort=int(port)
        )
        print(f"Port opened with IP {IP}.")
    except ClientError as e:
        print(f"Unexpected error: {e}.")


def main():
    key, secret, cluster_type, num_nodes, node_type, cluster_identifier, \
        db_name, db_user, db_password, db_port, iam_role_name \
        = get_cluster_params("dwh.cfg")
    ARN = get_roleARN(iam_role_name, key, secret)
    cluster_properties = create_redshift_cluster(
        key,
        secret,
        cluster_type,
        node_type,
        num_nodes,
        cluster_identifier,
        db_name,
        db_user,
        db_password,
        ARN
    )
    open_tcp_port(key, secret, db_port, cluster_properties)


if __name__ == "__main__":
    main()

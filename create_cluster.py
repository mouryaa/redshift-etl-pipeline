import pandas as pd
import boto3
import json
import psycopg2
from botocore.exceptions import ClientError
import configparser
import time


def create_iam_role(iam, DWH_IAM_ROLE_NAME):
    """
    Creates IAM Role for Redshift, to allow it to use AWS services
    """

    try:
        print("1.1 Creating a new IAM Role")
        dwhRole = iam.create_role(
            Path="/",
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"Service": "redshift.amazonaws.com"},
                        }
                    ],
                    "Version": "2012-10-17",
                }
            ),
        )
    except Exception as e:
        print(e)

    print("1.2 Attaching Policy")

    iam.attach_role_policy(
        RoleName=DWH_IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    )["ResponseMetadata"]["HTTPStatusCode"]

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)["Role"]["Arn"]

    print(roleArn)
    return roleArn


def create_cluster(
    redshift,
    roleArn,
    DWH_CLUSTER_TYPE,
    DWH_NODE_TYPE,
    DWH_NUM_NODES,
    DWH_DB,
    DWH_CLUSTER_IDENTIFIER,
    DWH_DB_USER,
    DWH_DB_PASSWORD,
):
    """
    Creates Redshift cluster
    """

    try:
        response = redshift.create_cluster(
            # HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            # Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            # Roles (for s3 access)
            IamRoles=[roleArn],
        )
        print("Redshift cluster creation http response status code: ")
        print(response["ResponseMetadata"]["HTTPStatusCode"])
        return response["ResponseMetadata"]["HTTPStatusCode"] == 200
    except Exception as e:
        print(e)
    return False


def get_cluster_props(redshift, DWH_CLUSTER_IDENTIFIER):
    """
    Retrieve Redshift clusters properties
    """

    def prettyRedshiftProps(props):
        pd.set_option("display.max_colwidth", -1)
        keysToShow = [
            "ClusterIdentifier",
            "NodeType",
            "ClusterStatus",
            "MasterUsername",
            "DBName",
            "Endpoint",
            "NumberOfNodes",
            "VpcId",
        ]
        x = [(k, v) for k, v in props.items() if k in keysToShow]
        return pd.DataFrame(data=x, columns=["Key", "Value"])

    myClusterProps = redshift.describe_clusters(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
    )["Clusters"][0]
    prettyRedshiftProps(myClusterProps)

    DWH_ENDPOINT = myClusterProps["Endpoint"]["Address"]
    DWH_ROLE_ARN = myClusterProps["IamRoles"][0]["IamRoleArn"]
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)

    config = configparser.ConfigParser()

    with open("dwh.cfg") as configfile:
        config.read_file(configfile)

    config.set("CLUSTER", "HOST", DWH_ENDPOINT)
    config.set("IAM_ROLE", "ARN", DWH_ROLE_ARN)

    with open("dwh.cfg", "w+") as configfile:
        config.write(configfile)

    return myClusterProps, DWH_ENDPOINT, DWH_ROLE_ARN


def get_redshift_cluster_status(redshift, DWH_CLUSTER_IDENTIFIER):
    """
    Retrieves the Redshift cluster status
    :param redshift: The Redshift resource client
    :return: The cluster status
    """
    cluster_props = redshift.describe_clusters(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
    )["Clusters"][0]
    cluster_status = cluster_props["ClusterStatus"]
    return cluster_status.lower()


def check_cluster_creation(redshift, DWH_CLUSTER_IDENTIFIER):
    """
    Check if the cluster status is available, if it is returns True. Otherwise, false.
    :param redshift: The Redshift client resource
    :return:bool
    """
    if get_redshift_cluster_status(redshift, DWH_CLUSTER_IDENTIFIER) == "available":
        return True
    return False


def open_ports(ec2, myClusterProps, DWH_PORT):
    """
    Update clusters security group to allow access through redshift port
    """

    try:
        vpc = ec2.Vpc(id=myClusterProps["VpcId"])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp="0.0.0.0/0",
            IpProtocol="TCP",
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT),
        )
    except Exception as e:
        print(e)


def main():

    config = configparser.ConfigParser()
    config.read_file(open("dwh.cfg"))

    KEY = config.get("AWS", "KEY")
    SECRET = config.get("AWS", "SECRET")

    DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
    DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
    DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
    DWH_DB = config.get("DWH", "DWH_DB")
    DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
    DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
    DWH_PORT = config.get("DWH", "DWH_PORT")

    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

    (DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

    df = pd.DataFrame(
        {
            "Param": [
                "DWH_CLUSTER_TYPE",
                "DWH_NUM_NODES",
                "DWH_NODE_TYPE",
                "DWH_CLUSTER_IDENTIFIER",
                "DWH_DB",
                "DWH_DB_USER",
                "DWH_DB_PASSWORD",
                "DWH_PORT",
                "DWH_IAM_ROLE_NAME",
            ],
            "Value": [
                DWH_CLUSTER_TYPE,
                DWH_NUM_NODES,
                DWH_NODE_TYPE,
                DWH_CLUSTER_IDENTIFIER,
                DWH_DB,
                DWH_DB_USER,
                DWH_DB_PASSWORD,
                DWH_PORT,
                DWH_IAM_ROLE_NAME,
            ],
        }
    )

    print(df)

    ec2 = boto3.resource(
        "ec2",
        region_name="us-east-1",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )

    s3 = boto3.resource(
        "s3",
        region_name="us-east-1",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )

    iam = boto3.client(
        "iam",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
        region_name="us-east-1",
    )

    redshift = boto3.client(
        "redshift",
        region_name="us-east-1",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )

    roleArn = create_iam_role(iam, DWH_IAM_ROLE_NAME)

    clusterCreationStarted = create_cluster(
        redshift,
        roleArn,
        DWH_CLUSTER_TYPE,
        DWH_NODE_TYPE,
        DWH_NUM_NODES,
        DWH_DB,
        DWH_CLUSTER_IDENTIFIER,
        DWH_DB_USER,
        DWH_DB_PASSWORD,
    )

    if clusterCreationStarted:
        print("Cluster being created....")
        print("Wait....")
        time.sleep(600)

        if check_cluster_creation(redshift, DWH_CLUSTER_IDENTIFIER):
            myClusterProps = get_cluster_props(redshift, DWH_CLUSTER_IDENTIFIER)
            open_ports(ec2, myClusterProps, DWH_PORT)
            print("Wait....")
            time.sleep(300)
            conn = psycopg2.connect(
                "host={} dbname={} user={} password={} port={}".format(
                    *config["CLUSTER"].values()
                )
            )
            cur = conn.cursor()
            print("Connected")
            conn.close()


if __name__ == "__main__":
    main()

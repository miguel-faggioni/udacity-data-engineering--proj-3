#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Code to create the necessary resources for the ETL to run if don't exist.

This file creates:

  - IAM role with S3 access
  - Redshift cluster
"""

import pandas as pd
import boto3
import json
import configparser
from botocore.exceptions import ClientError
import time
import psycopg2

# open the `dwh.secret.cfg` file
config = configparser.ConfigParser()
config.read_file(open('dwh.secret.cfg'))

# load the file content into variables for later usage
KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

REGION                 = boto3.session.Session().region_name

# create access to resources
iam = boto3.client(
    'iam',
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
    region_name=REGION
)
redshift = boto3.client(
    'redshift',
    region_name=REGION,
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET
)

# get the IAM role
existingRoles = [role for role in iam.list_roles()['Roles']]
if any([role['RoleName'] == DWH_IAM_ROLE_NAME for role in existingRoles]) == True:
    # role already exists
    dwhRole = [role for role in existingRoles if role['RoleName'] == DWH_IAM_ROLE_NAME][0]
else:
    # role doesn't exist, create the IAM role
    try:
        print("Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps({
                'Statement': [{
                    'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {
                        'Service': 'redshift.amazonaws.com'
                    }
                }],
                'Version': '2012-10-17'
            })
        )['Role']
    except Exception as e:
        print(e)

# add access to S3 bucket
print("Attaching Policy")
iam.attach_role_policy(
    RoleName=DWH_IAM_ROLE_NAME,
    PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
)['ResponseMetadata']['HTTPStatusCode']

# get the ARN for the IAM role
print("Get the IAM role ARN")
roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
print(roleArn)

# create the Redshift cluster if it doesn't exist
existingClusters = [cluster for cluster in redshift.describe_clusters()['Clusters']]
if any([cluster['ClusterIdentifier'] == DWH_CLUSTER_IDENTIFIER for cluster in existingClusters]) == True:
    # cluster already exists
    dwhCluster = [cluster for cluster in existingClusters if cluster['ClusterIdentifier'] == DWH_CLUSTER_IDENTIFIER][0]
else:
    # cluster doesn't exist, create the Redshift cluster
    try:
        response = redshift.create_cluster(        
            #HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )
    except Exception as e:
        print(e)
    dwhCluster = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

# check the cluster status
def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', None)
    keysToShow = [
        "ClusterIdentifier",
        "NodeType",
        "ClusterStatus",
        "MasterUsername",
        "DBName",
        "Endpoint",
        "NumberOfNodes",
        'VpcId'
    ]
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])
prettyRedshiftProps(dwhCluster)

# wait for cluster to become available
print('Waiting for cluster to become available')
while dwhCluster['ClusterAvailabilityStatus'] != 'Available':
    print('Still not available')
    print('Trying again in 30 seconds')
    time.sleep(30)
    dwhCluster = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
print('Cluster is available')

# get the information of the DWH
DWH_ENDPOINT = dwhCluster['Endpoint']['Address']
DWH_ROLE_ARN = dwhCluster['IamRoles'][0]['IamRoleArn']
print("DWH_ENDPOINT :: ", {'Address': 'dwhcluster.cm8ea0dis6dd.us-west-2.redshift.amazonaws.com', 'Port': 5439})
print("DWH_ROLE_ARN :: ", 'arn:aws:iam::235044904284:role/myRedshiftRole')

# allow access to security group
ec2 = boto3.resource(
    'ec2',
    region_name=REGION,
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET
)
try:
    vpc = ec2.Vpc(id=dwhCluster['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
except Exception as e:
    print(e)

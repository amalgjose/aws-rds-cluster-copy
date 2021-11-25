#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse

import boto3
import time


def copy_rds_cluster(
        source_access_key_id,
        source_secret_access_key,
        source_region_name,
        dest_access_key_id,
        dest_secret_access_key,
        dest_region_name,
        db_cluster_name):
    """Copy RDS DB cluster from one AWS account to another using boto3.

        @type source_access_key_id: string
        @param source_access_key_id: ACCESS_KEY_ID of source account
        @type source_secret_access_key: string
        @param source_secret_access_key: SECRET_ACCESS_KEY of source account
        @type source_region_name: string
        @param source_region_name: REGION_NAME of source account
        @type dest_access_key_id: string
        @param dest_access_key_id: ACCESS_KEY_ID of source account
        @type dest_secret_access_key: string
        @param dest_secret_access_key: SECRET_ACCESS_KEY of source account
        @type dest_region_name: string
        @param dest_region_name: REGION_NAME of source account
        @param db_cluster_name: DB cluster name on source account"""

    # Create source and destination account sessions
    source_session = boto3.session.Session(
        aws_access_key_id=source_access_key_id,
        aws_secret_access_key=source_secret_access_key,
        region_name=source_region_name)

    dest_session = boto3.session.Session(
        aws_access_key_id=dest_access_key_id,
        aws_secret_access_key=dest_secret_access_key,
        region_name=dest_region_name)

    # Get source and destination AWS account IDs
    source_iam = source_session.client("iam")
    source_aws_account_id = source_iam.get_user()["User"]["Arn"].split(":")[4]

    dest_iam = dest_session.client('iam')
    dest_aws_account_id = dest_iam.get_user()["User"]["Arn"].split(":")[4]

    # Define DB snapshot name
    snapshot_name = "{}-snapshot".format(db_cluster_name)

    # Create DB cluster snapshot on source account
    source_rds = source_session.client("rds")
    source_rds.create_db_cluster_snapshot(
        DBClusterSnapshotIdentifier=snapshot_name,
        DBClusterIdentifier=db_cluster_name,
    )
    print("Waiting to create DB cluster snapshot...")
    waiter = source_rds.get_waiter('db_cluster_snapshot_available')
    waiter.wait()

    # Share DB cluster snapshot with destination account
    source_rds.modify_db_cluster_snapshot_attribute(
        DBClusterSnapshotIdentifier=snapshot_name,
        AttributeName='restore',
        ValuesToAdd=[dest_aws_account_id, ])
    print("=========Cluster snapshot creation completed==========")
    snapshot_arn = "arn:aws:rds:{}:{}:cluster-snapshot:{}".format(
        source_region_name,
        source_aws_account_id,
        snapshot_name)
    print("RDS cluster snapshot ARN-->", snapshot_arn)
    # Restore DB instance from DB snapshot
    dest_rds = dest_session.client("rds")
    print("==============Restoring the snapshot===============")
    # update the database engine, database engine version in the below code. This is mandatory
    # https://docs.aws.amazon.com/AmazonRDS/latest/APIReference/API_DescribeDBEngineVersions.html
    dest_rds.restore_db_cluster_from_snapshot(DBClusterIdentifier=db_cluster_name,
                                               SnapshotIdentifier=snapshot_arn,
                                               Engine="<db-engine>",
                                               EngineVersion="<database-engine>")
    time.sleep(60)
    # update the db instance type and database engine version in the below code. This is mandatory
    dest_rds.create_db_instance(
        DBClusterIdentifier=db_cluster_name,
        DBInstanceIdentifier=db_cluster_name + '-rds',
        DBInstanceClass='<db instance type>',
        PubliclyAccessible=True,
        DBSubnetGroupName='default',
        Engine='aurora',
        EngineVersion='<database-engine>'
    )
    print("Waiting to restore DB cluster from snapshot...")


if __name__ == "__main__":
    """
        Update the below line with the Access Key, Secret Key and Region of the 
        source aws account
    """
    source_credentials = "<SOURCE_ACCESS_KEY>:<SOURCE_SECRET_KEY>:<REGION>"
    sc = source_credentials.split(":")
    source_access_key_id, source_secret_access_key, source_region_name = sc
    """
        Update the below line with the Access Key, Secret Key and Region of the 
        target aws account
    """
    destination_credentials =  "<DEST_ACCESS_KEY>:<DEST_SECRET_KEY>:<REGION>"
    dc = destination_credentials.split(":")
    dest_access_key_id, dest_secret_access_key, dest_region_name = dc

    # Update the below line with your cluster name
    db_cluster_name = "rds-db-cluster-name"

    # Call function with arguments
    copy_rds_cluster(
        source_access_key_id,
        source_secret_access_key,
        source_region_name,
        dest_access_key_id,
        dest_secret_access_key,
        dest_region_name,
        db_cluster_name)

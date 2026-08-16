import os
import boto3
import time
from botocore.exceptions import ClientError


def query_glue_table_partitions(table_name: str,
                                partition_path: str,
                                aws_access_key_id: str,
                                aws_secret_access_key: str,
                                aws_region: str = 'us-west-2'
                               ) -> bool:
    """
    Query AWS Glue Data Catalog to check if a specific partition exists for a table.

    Args:
        table_name: Format 'database.table'
        partition_path: Partition filter like 'season=2023'
        aws_access_key_id: AWS access key
        aws_secret_access_key: AWS secret key
        aws_region: AWS region

    Returns:
        bool: True if partition exists, False otherwise
    """
    try:
        database_name = table_name.split('.')[0]
        table_name_only = table_name.split('.')[1]

        glue_client = boto3.client(
            'glue',
            region_name=aws_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

        # Parse partition values from partition_path (e.g., 'season=2023' -> {'season': '2023'})
        partition_values = {}
        if '=' in partition_path:
            key, value = partition_path.split('=', 1)
            partition_values[key] = value

        # Get table info to understand partition keys
        try:
            table_response = glue_client.get_table(
                DatabaseName=database_name,
                Name=table_name_only
            )

            partition_keys = table_response['Table'].get('PartitionKeys', [])
            if not partition_keys:
                # Table is not partitioned
                print(f"Table {table_name} is not partitioned")
                return True

        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                print(f"Table {table_name} does not exist in Glue catalog")
                return False
            raise

        # Check if specific partition exists
        if partition_values:
            try:
                # Build partition values list in the correct order
                partition_value_list = []
                for pk in partition_keys:
                    key_name = pk['Name']
                    if key_name in partition_values:
                        partition_value_list.append(partition_values[key_name])
                    else:
                        print(f"Partition key {key_name} not found in partition path {partition_path}")
                        return False

                glue_client.get_partition(
                    DatabaseName=database_name,
                    TableName=table_name_only,
                    PartitionValues=partition_value_list
                )
                return True

            except ClientError as e:
                if e.response['Error']['Code'] == 'EntityNotFoundException':
                    return False
                raise
        else:
            # If no specific partition values provided, check if table has any partitions
            try:
                response = glue_client.get_partitions(
                    DatabaseName=database_name,
                    TableName=table_name_only,
                    MaxResults=1
                )
                return len(response.get('Partitions', [])) > 0
            except ClientError as e:
                if e.response['Error']['Code'] == 'EntityNotFoundException':
                    return False
                raise

    except Exception as e:
        print(f"Error querying Glue catalog for table {table_name}, partition {partition_path}: {str(e)}")
        return False


def poke_glue_partition(table, partition, aws_access_key_id, aws_secret_access_key, aws_region='us-west-2'):
    """
    Poll AWS Glue Data Catalog until a specific partition is found.

    Args:
        table: Table name in format 'database.table'
        partition: Partition filter like 'season=2023'
        aws_access_key_id: AWS access key
        aws_secret_access_key: AWS secret key
        aws_region: AWS region
    """
    found_partition = query_glue_table_partitions(table, partition, aws_access_key_id, aws_secret_access_key, aws_region)
    while not found_partition:
        print(f'Partition {partition} for table {table} not found in Glue catalog! Trying again in a minute!')
        time.sleep(60)
        found_partition = query_glue_table_partitions(table, partition, aws_access_key_id, aws_secret_access_key, aws_region)





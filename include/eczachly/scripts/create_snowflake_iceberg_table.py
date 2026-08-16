from include.eczachly.snowflake_queries import get_snowpark_session
import os


def create_snowflake_iceberg_table(
    source_table: str = None,
    snowflake_table: str = "polygon_stock_prices_iceberg",
    schema: str = "bootcamp"
):
    """
    Create an Iceberg table in Snowflake based on AWS Glue Iceberg table metadata.
    
    Args:
        source_table: Source Iceberg table in AWS Glue (namespace.table_name)
        snowflake_table: Target table name in Snowflake
        schema: Snowflake schema name
    """
    # Set default source_table if not provided
    if source_table is None:
        source_table = f"{os.environ.get('STUDENT_SCHEMA', 'zachwilson')}.polygon_stock_prices"
    
    session = get_snowpark_session(schema=schema)
    
    # Get S3 bucket from environment
    s3_bucket = os.environ.get('AWS_S3_BUCKET_TABULAR', 'zachwilsonsorganization-522')
    aws_region = os.environ.get('AWS_REGION', 'us-west-2')
    
    # Parse source table
    namespace, table_name = source_table.split('.')
    
    # Construct the Iceberg table location in S3
    iceberg_location = f"s3://{s3_bucket}/iceberg-warehouse/{namespace}/{table_name}"
    
    # Create external volume if it doesn't exist
    create_volume_ddl = f"""
    CREATE EXTERNAL VOLUME IF NOT EXISTS iceberg_external_volume
    STORAGE_LOCATIONS = (
        (
            NAME = 'iceberg-s3-volume'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://{s3_bucket}/iceberg-warehouse/'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::YOUR_ACCOUNT_ID:role/snowflake-iceberg-role'
        )
    );
    """
    
    try:
        session.sql(create_volume_ddl).collect()
        print(f"External volume created or already exists")
    except Exception as e:
        print(f"Note: External volume creation may require admin privileges: {str(e)}")
    
    # Create Iceberg table in Snowflake
    # This assumes the Iceberg table already exists in AWS Glue
    create_iceberg_table_ddl = f"""
    CREATE OR REPLACE ICEBERG TABLE {schema}.{snowflake_table}
    EXTERNAL_VOLUME = 'iceberg_external_volume'
    CATALOG = 'SNOWFLAKE'
    METADATA_FILE_PATH = '{iceberg_location}/metadata/'
    AUTO_REFRESH = TRUE
    ;
    """
    
    try:
        session.sql(create_iceberg_table_ddl).collect()
        print(f"Successfully created Iceberg table {schema}.{snowflake_table}")
    except Exception as e:
        # If direct metadata path doesn't work, try catalog integration approach
        print(f"Direct metadata approach failed, trying catalog integration: {str(e)}")
        
        # Alternative: Create catalog integration for AWS Glue
        create_catalog_integration = f"""
        CREATE OR REPLACE CATALOG INTEGRATION aws_glue_catalog
        CATALOG_SOURCE = GLUE
        TABLE_FORMAT = ICEBERG
        GLUE_AWS_ROLE_ARN = 'arn:aws:iam::YOUR_ACCOUNT_ID:role/snowflake-glue-role'
        GLUE_CATALOG_ID = 'YOUR_AWS_ACCOUNT_ID'
        GLUE_REGION = '{aws_region}'
        ENABLED = TRUE;
        """
        
        try:
            session.sql(create_catalog_integration).collect()
            print("Catalog integration created")
        except Exception as e2:
            print(f"Catalog integration creation note: {str(e2)}")
        
        # Create Iceberg table using catalog integration
        create_table_with_catalog = f"""
        CREATE OR REPLACE ICEBERG TABLE {schema}.{snowflake_table}
        CATALOG = 'aws_glue_catalog'
        EXTERNAL_VOLUME = 'iceberg_external_volume'
        CATALOG_TABLE_NAME = '{namespace}.{table_name}'
        AUTO_REFRESH = TRUE;
        """
        
        session.sql(create_table_with_catalog).collect()
        print(f"Successfully created Iceberg table {schema}.{snowflake_table} using catalog integration")
    
    # Query the table to verify
    try:
        result = session.sql(f"SELECT COUNT(*) as count FROM {schema}.{snowflake_table}").collect()
        count = result[0]['COUNT']
        print(f"Table contains {count} rows")
        return f"Successfully created Iceberg table {schema}.{snowflake_table} with {count} rows"
    except Exception as e:
        print(f"Table created but verification failed: {str(e)}")
        return f"Iceberg table {schema}.{snowflake_table} created (verification pending)"


if __name__ == "__main__":
    create_snowflake_iceberg_table()

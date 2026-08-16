import pytest
from pyspark.sql import SparkSession
from databricks.connect import DatabricksSession

@pytest.fixture(scope='module')
def spark() -> SparkSession:
  # Create a SparkSession (the entry point to Spark functionality) on
  # the cluster in the remote Databricks workspace. Unit tests do not
  # have access to this SparkSession by default.
  spark = DatabricksSession.builder.getOrCreate()
  return spark 
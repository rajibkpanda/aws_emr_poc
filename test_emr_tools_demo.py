import unittest
from unittest.mock import patch
from emr_tools_demo import run

class TestRun(unittest.TestCase):

    @patch('pyspark.sql.SparkSession.builder')
    def test_run(self, mock_builder):
        mock_spark = mock_builder.appName.return_value
        mock_spark.config.return_value = mock_spark
        mock_spark.getOrCreate.return_value = mock_spark

        S3_DATA_SOURCE_PATH = 's3://rajib-glue-test/employee-poc/input/employee_basic_details.csv'
        S3_DATA_OUTPUT_PATH = 's3://rajib-glue-test/employee-poc/output'

        mock_df = mock_spark.read.csv.return_value

        run()

        mock_builder.appName.assert_called_with("EMRLocal")
        mock_spark.config.assert_called_with("spark.sql.catalogImplementation", "hive")
        mock_spark.config.assert_called_with(
            "spark.hadoop.hive.metastore.client.factory.class",
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
        )
        mock_spark.getOrCreate.assert_called_with()
        mock_spark.read.csv.assert_called_with(S3_DATA_SOURCE_PATH, header=True)
        mock_df.show.assert_called_with()
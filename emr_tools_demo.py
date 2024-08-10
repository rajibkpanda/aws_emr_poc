from pyspark.sql import SparkSession

def run() :

    spark = (
            SparkSession.builder.appName("EMRLocal")
            # Uncomment these lines to enable the Glue Data Catalog
            .config("spark.sql.catalogImplementation", "hive")
            .config(
                "spark.hadoop.hive.metastore.client.factory.class",
                "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
            )
            .getOrCreate()
        )

    S3_DATA_SOURCE_PATH = 's3://rajib-glue-test/employee-poc/input/employee_basic_details.csv'
    S3_DATA_OUTPUT_PATH = 's3://rajib-glue-test/employee-poc/output'

    df = spark.read.csv(S3_DATA_SOURCE_PATH, header=True)

    print(df.head())
    df.show()

    # spark.stop()

if __name__ == "__main__":
    run()

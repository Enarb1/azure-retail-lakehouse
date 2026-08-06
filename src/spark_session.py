from pyspark.sql import SparkSession

def get_spark(app_name: str = "AzureRetailLakehouse") -> SparkSession:
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    return spark
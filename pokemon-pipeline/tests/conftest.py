import pytest
from pyspark.sql import SparkSession 
import json


@pytest.fixture(scope="session")
def spark():

    spark = (SparkSession.builder
         .master("local")
         .appName("Word Count")
         .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
     .getOrCreate()
    )
    
    return spark

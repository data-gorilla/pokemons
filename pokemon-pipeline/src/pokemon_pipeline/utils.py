from pyspark.sql import SparkSession


def init_spark_session():

    spark = (SparkSession.builder
         .master("local")
         .appName("Word Count")
         .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
     .getOrCreate()
    )
    
    return spark

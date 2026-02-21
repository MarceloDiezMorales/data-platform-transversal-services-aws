"""
Module for Spark setup in AWS Glue with S3A and Iceberg configuration.
"""
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from typing import Tuple
from awsglue.job import Job        

def initialize_spark(key_output: str) -> Tuple[GlueContext, object, Job]:
    """
    Initialize Spark context for AWS Glue with proper S3A and Iceberg configuration.

    Args:
        key_output (str): S3 path for Iceberg warehouse.

    Returns:
        Tuple[GlueContext, SparkSession, Job]: 
        Initialized Glue context, Spark session, and Glue job.
    """
    # Get or create SparkContext
    sc = SparkContext.getOrCreate()

    # Spark configuration
    conf = sc.getConf()
    conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # S3 configuration
    conf.set("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
    conf.set("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")

    # Iceberg catalog configuration (Glue)
    conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    conf.set("spark.sql.catalog.glue_catalog.catalog-impl",
             "org.apache.iceberg.aws.glue.GlueCatalog")
    conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    conf.set("spark.sql.catalog.glue_catalog.warehouse", key_output)
    conf.set("spark.sql.defaultCatalog", "glue_catalog")
    conf.set("spark.sql.catalog.glue_catalog.http-client.apache.max-connections", "3000")

    # Apply configuration
    sc.stop()
    sc = SparkContext.getOrCreate(conf=conf)

    glue_context = GlueContext(sc)
    spark_session = glue_context.spark_session
    job = Job(glue_context)

    return glue_context, spark_session, job

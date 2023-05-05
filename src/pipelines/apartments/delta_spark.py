import to_s3
import scraping_solids

import delta
import pyspark.sql


def local_fs():
    return (
        pyspark.sql.SparkSession.builder.appName("apartment_hunting")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.access.key=minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key=minioadmin")
        .remote("sc://127.0.0.1:9000")
    )


def remote_fs(remote, acc_key, sec_key, bucket_creation):
    return (
        pyspark.sql.SparkSession.builder.appName("apartment_hunting")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .remote("sc://" + remote)
        .config("spark.hadoop.fs.s3a.access.key=" + acc_key)
        .config("spark.hadoop.fs.s3a.secret.key=" + sec_key)
    )


builder = local_fs()

spark = delta.configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.parallelize()
spark.write.format("delta").save("/data/test_scrape")

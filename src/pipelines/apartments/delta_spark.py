# import to_s3
import scraping_solids

import delta
import pyspark
import pyspark.sql as pssql
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    ArrayType,
)


def local_fs():
    return (
        pssql.SparkSession.builder.appName("apartment_hunting")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000")
        # .remote("sc://127.0.0.1:9000")
    )


def remote_fs(acc_key, sec_key, remote):
    return (
        pssql.SparkSession.builder.appName("apartment_hunting")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.access.key", acc_key)
        .config("spark.hadoop.fs.s3a.secret.key", sec_key)
        .config("spark.hadoop.fs.s3a.endpoint", remote)
    )


builder = local_fs()

spark = delta.configure_spark_with_delta_pip(
    builder,
    [
        "org.apache.hadoop:hadoop-aws:3.3.2",
        "com.amazonaws:aws-java-sdk-bundle:1.12.472",
    ],
).getOrCreate()
# rdd = spark.sparkContext.parallelize(scraping_solids.get_listings(scraping_solids.page))

# df = rdd.toDF(["names", "prices", "beds", "amenities", "links"])
listings = scraping_solids.listings

schema = StructType(
    [
        StructField("name", StringType(), True),
        StructField("price", StringType(), True),
        StructField("beds", StringType(), True),
        StructField("amenities", ArrayType(StringType(), True), True),
        StructField("link", StringType(), True),
    ]
)

df = spark.createDataFrame(listings, schema=schema)

df.show()
df.printSchema()
# df = spark.createDataFrame([[1, 1, 1], [2, 2, 2], [3, 3, 3]], ["one", "two", "three"])

# print(df)
# print(df.head())
df.write.format("delta").save("s3a://data/test_scrape")

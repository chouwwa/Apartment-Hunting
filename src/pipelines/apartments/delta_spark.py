# import to_s3
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
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        # .remote("sc://127.0.0.1:9000")
    )


def remote_fs(acc_key, sec_key):
    return (
        pyspark.sql.SparkSession.builder.appName("apartment_hunting")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.access.key", acc_key)
        .config("spark.hadoop.fs.s3a.secret.key", sec_key)
        # .remote("sc://" + remote)
    )


builder = local_fs()

spark = delta.configure_spark_with_delta_pip(builder).getOrCreate()
# rdd = spark.sparkContext.parallelize(scraping_solids.get_listings(scraping_solids.page))

# df = rdd.toDF(["names", "prices", "beds", "amenities", "links"])
# df = spark.createDataFrame(
#     scraping_solids.get_listings(scraping_solids.page),
#     ["names", "prices", "beds", "amenities", "links"],
# )

df = spark.createDataFrame([[1, 1, 1], [2, 2, 2], [3, 3, 3]], ["one", "two", "three"])

print(df)
print(df.head())
# df.write.format("delta").save("s3a://127.0.0.1:9000/data/test_scrape")

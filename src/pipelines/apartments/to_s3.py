import scraping_solids

from minio import Minio
from minio.error import S3Error
import pyarrow as pa
from pyarrow import fs, csv, parquet


def local_fs():
    return fs.S3FileSystem(
        endpoint_override="http://127.0.0.1:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        allow_bucket_creation=True,
    )


def remote_fs(remote, acc_key, sec_key, bucket_creation):
    return fs.S3FileSystem(
        endpoint_override=remote,
        access_key=acc_key,
        secret_key=sec_key,
        allow_bucket_creation=bucket_creation,
    )


def write_parquet_s3(obj, path, filename, filesystem):
    parquet.write_table(
        obj,
        path + filename + ".parquet",
        filesystem=filesystem,
    )


def read_parquet_s3(path, filename, filesystem):
    return parquet.read_table(
        path + filename + ".parquet",
        filesystem=filesystem,
    )


# client = local_fs()

# client.create_dir("data")
# write_parquet_s3(
#     scraping_solids.get_listings_pyarrow(scraping_solids.page),
#     "data/",
#     "test_scrape",
#     client,
# )

# print(read_parquet_s3("data/", "test_scrape", client))

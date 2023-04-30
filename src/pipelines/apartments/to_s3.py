from minio import Minio
from minio.error import S3Error
from pyarrow import fs, csv, parquet

client = fs.S3FileSystem(
    endpoint_override="play.min.io",
    access_key="minioadmin",
    secret_key="minioadmin",
)

if client.bucket_exists("data"):
    print("data folder exists")
else:
    client.make_bucket("data")

client.fput_object("data", "first_page", )


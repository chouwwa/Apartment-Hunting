from minio import Minio
from minio.error import S3Error
import pyarrow as pa
from pyarrow import fs, csv, parquet

client = fs.S3FileSystem(
    endpoint_override="http://127.0.0.1:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
)

if client.bucket_exists("data"):
    print("data folder exists")
else:
    client.make_bucket("data")

pa.Table.from_dict()


client.fput_object(
    "data",
    "first_page",
)

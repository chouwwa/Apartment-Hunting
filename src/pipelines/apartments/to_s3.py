from minio import Minio
from minio.error import S3Error

client = Minio(
    "play.min.io",
    access_key="minioadmin",
    secret_key="minioadmin",
)

if client.bucket_exists("data"):
    print("data folder exists")
else:
    client.make_bucket("data")

client.fput_object("data", "first_page", "")

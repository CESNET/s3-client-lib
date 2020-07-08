from s3_client_lib.s3_client import S3Client
from s3_client_lib.s3_multipart_client import S3MultipartClient
import logging

FORMAT = '%(asctime)-15s %(levelname)s %(message)s'
logging.basicConfig(filename='example.log', filemode='w',format=FORMAT, level=logging.DEBUG)

s3_config_localstack = {
    "AWS_S3_ENDPOINT_URL": "http://localhost:4566",
    "AWS_SECRET_ACCESS_KEY": "adsf",
    "AWS_ACCESS_KEY_ID": "asdff"
}
# for large files#
#client = S3MultipartClient(s3_config_localstack["AWS_S3_ENDPOINT_URL"],
#                  s3_config_localstack["AWS_ACCESS_KEY_ID"],
#                  s3_config_localstack["AWS_SECRET_ACCESS_KEY"])

# for files with size < 1GB
client = S3Client(s3_config_localstack["AWS_S3_ENDPOINT_URL"],
                  s3_config_localstack["AWS_ACCESS_KEY_ID"],
                  s3_config_localstack["AWS_SECRET_ACCESS_KEY"])

if __name__ == '__main__':  
    client.create_bucket_if_not_exists("bucket")
    print(client.upload_local_file("path to object .zip", "bucket", "test"))
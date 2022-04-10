from s3_client_lib.s3_multipart_client import S3MultipartClient
import logging
import requests
from s3_client_lib.utils import CHUNK_SIZE_128M

FORMAT = "%(asctime)-15s %(levelname)s %(message)s"
logging.basicConfig(
    filename="example.log", filemode="w", format=FORMAT, level=logging.DEBUG
)
logger = logging.getLogger()

s3_config_localstack_main = {
    "AWS_S3_ENDPOINT_URL": "http://localhost:4566",
    "AWS_SECRET_ACCESS_KEY": "adsf",
    "AWS_ACCESS_KEY_ID": "asdff",
}


s3_config_localstack_backup = {
    "AWS_S3_ENDPOINT_URL": "http://localhost:4566",
    "AWS_SECRET_ACCESS_KEY": "adsf",
    "AWS_ACCESS_KEY_ID": "asdff",
}

client_main = S3MultipartClient(
    s3_config_localstack_main["AWS_S3_ENDPOINT_URL"],
    s3_config_localstack_main["AWS_ACCESS_KEY_ID"],
    s3_config_localstack_main["AWS_SECRET_ACCESS_KEY"],
)

client_backup = S3MultipartClient(
    s3_config_localstack_backup["AWS_S3_ENDPOINT_URL"],
    s3_config_localstack_backup["AWS_ACCESS_KEY_ID"],
    s3_config_localstack_backup["AWS_SECRET_ACCESS_KEY"],
)

def random_file(path):
    import os
    with open(path, 'wb') as fout:
        fout.write(os.urandom(1024))


if __name__ == "__main__":
    ## setup of the test
    main_bucket = "bucket"
    backup_bucket = "bucketbackup"
    object_name = "test"
    client_main.create_bucket_if_not_exists(main_bucket)
    client_backup.create_bucket_if_not_exists(backup_bucket)
    random_file("/tmp/test_file")
    print(client_main.upload_local_file("/tmp/test_file", main_bucket, object_name))
    client_main.update_metadata_object(
        main_bucket, object_name, metadata={"test_metadata": "metadata"}
    )

    # prepare multipart uplaod for backup
    upload_id = client_backup.create_multipart_upload(
        backup_bucket, object_name
    )
    part_no = 1
    parts = []
    # get binary data from main bucket
    response = client_main.get_object(main_bucket, object_name)
    for idx, chunk in enumerate(response['Body'].iter_chunks(CHUNK_SIZE_128M)):
        part_no = idx + 1
        # sign part
        url = client_backup.sign_part_upload(
            backup_bucket, object_name, upload_id, part_no
        )
        # upload part via signed url
        res = requests.put(url, data=chunk)
        logger.info(f'part=#{part_no} headers: {res.headers} {dir(res)}')
        etag = res.headers.get('ETag', '')
        logger.info(f"part=#{part_no} {etag}")
        # store etag to parts list
        parts.append({'ETag': etag.replace('"', ''), 'PartNumber': part_no})
    logger.info(f'finishing {backup_bucket} - {object_name} parts={parts}, upload_id={upload_id}')
    # finish uploading part
    res = client_backup.finish_multipart_upload(
        backup_bucket,
        object_name,
        parts,
        upload_id,
    )
    logger.info(f"result of finishing multipart {res}")
    # get metadata if object has some
    s3_object = client_main.get_object_head(main_bucket, object_name)
    obj_metadata = s3_object.get('Metadata', {})
    if any(obj_metadata):
        result = client_backup.update_metadata_object(
            backup_bucket, object_name, obj_metadata
        )

    # check
    s3_object = client_backup.get_object_head(backup_bucket, object_name)
    logger.info(f"Finishing copy of the object between S3 {s3_object}")
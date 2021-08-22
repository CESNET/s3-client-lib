
import logging
import requests
import time

logger = logging.getLogger(__name__)
CHUNK_SIZE_16M = 16777216
CHUNK_SIZE_32M = 33554432
CHUNK_SIZE_128M = 134217728
MB_512 = 536870912
GB_1 = 1073741274
GB_5 = 5368706371
GB_10 = 10737412742
GB_100 = 107374127424

def read_in_chunks(file_object, chunk_size=CHUNK_SIZE_16M):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1k."""
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data


def get_file_chunk_size(file_size):
    def getnumchunks(file_size, max_size):
        num = int(file_size / max_size)
        if file_size % max_size:
            num += 1
        return num
    if file_size < MB_512:
        return 1, file_size
    elif file_size < GB_10:
        return getnumchunks(file_size, CHUNK_SIZE_16M), CHUNK_SIZE_16M
    elif file_size < GB_100:
        return getnumchunks(file_size, CHUNK_SIZE_128M), CHUNK_SIZE_128M
    else:
        return getnumchunks(file_size, MB_512), MB_512


def create_presigned_upload_part(client, bucket, key, upload_id, part_no, expires):
    return client.generate_presigned_url(ClientMethod='upload_part',
                                         Params={'Bucket': bucket,
                                                 'Key': key,
                                                 'UploadId': upload_id,
                                                 'PartNumber': part_no},
                                                 ExpiresIn=expires)


def upload_part_(client, bucket, key, upload_id, part_no, part, expires):
    signed_url = create_presigned_upload_part(client, bucket, key, upload_id, part_no, expires)
    logger.info(f"Uploading part [{part_no}]...")
    logger.debug(f"[{part_no}] Presigned url {signed_url}")
    res = requests.put(signed_url, data=part)
    logger.debug(f"headers: {res.headers}")
    etag = res.headers.get('ETag')
    logger.debug(f"part: [{part_no}] Etag {etag}")
    return {'ETag': etag, 'PartNumber': part_no}  # you have to append etag and partnumber of each parts


def upload_part(url, file_name, cursor, part_no, chunk_size):
    """
    Function will try to upload chunk of data read from file to S3 via presigned url
    There are 5 attempts to upload chunk if something failed in upload

    :param url: presigned url of chunk
    :param file_name: full path of file
    :param cursor: from where to start read to chunk_size
    :param part_no: part number
    :param chunk_size: size of chunk which will be uploaded
    :return: {'ETag': "Etag", 'PartNumber': part_no}
    """
    with open(file_name, "rb") as rf:
        rf.seek(cursor)
        data = rf.read(chunk_size)
        if data is None or len(data) == 0:
            return None
        logger.info(f"Uploading part [{part_no}]...")
        for i in range(0, 5):
            try:
                logger.info(f"Uploading part [{part_no}] to url: {url}...")
                res = requests.put(url, data=data)
                if "Connection" in res.headers and res.headers["Connection"] == "close":
                    continue
                logger.debug(f"{part_no} - headers: {res.headers}")
                etag = res.headers.get('ETag', "")
                return {'ETag': etag.replace("\"", ""), 'PartNumber': part_no}
            except Exception as e:
                logger.error(f"Error {e} tryies: {i}")
                time.sleep(1)
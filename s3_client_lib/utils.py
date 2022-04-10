import logging
import requests
import time
import io

logger = logging.getLogger(__name__)
CHUNK_SIZE_16M = 16_777_216
CHUNK_SIZE_32M = 33_554_432
CHUNK_SIZE_128M = 134_217_728
MB_512 = 536_870_912
GB_1 = 1_073_741_274
GB_5 = 5_368_706_371
GB_10 = 10_737_412_742
GB_100 = 107_374_127_424

class S3File(io.RawIOBase):
    def __init__(self, s3_object):
        self.s3_object = s3_object
        self.position = 0
        self.chunk_size = CHUNK_SIZE_128M

    def __repr__(self):
        return '<%s s3_object=%r>' % (type(self).__name__, self.s3_object)

    @property
    def size(self):
        return self.s3_object.content_length

    def tell(self):
        return self.position

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_SET:
            self.position = offset
        elif whence == io.SEEK_CUR:
            self.position += offset
        elif whence == io.SEEK_END:
            self.position = self.size + offset
        else:
            raise ValueError(
                'invalid whence (%r, should be %d, %d, %d)'
                % (whence, io.SEEK_SET, io.SEEK_CUR, io.SEEK_END)
            )

        return self.position

    def seekable(self):
        return True

    def read(self, size=-1):
        if size == -1:
            # Read to the end of the file
            range_header = 'bytes=%d-' % self.position
            self.seek(offset=0, whence=io.SEEK_END)
        else:
            new_position = self.position + size

            # If we're going to read beyond the end of the object, return
            # the entire object.
            if new_position >= self.size:
                return self.read()

            range_header = 'bytes=%d-%d' % (self.position, new_position - 1)
            self.seek(offset=size, whence=io.SEEK_CUR)

        return self.s3_object.get(Range=range_header)['Body'].read()

    def readable(self):
        return True


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
    return client.generate_presigned_url(
        ClientMethod='upload_part',
        Params={
            'Bucket': bucket,
            'Key': key,
            'UploadId': upload_id,
            'PartNumber': part_no,
        },
        ExpiresIn=expires,
    )


def upload_part_(client, bucket, key, upload_id, part_no, part, expires):
    signed_url = create_presigned_upload_part(
        client, bucket, key, upload_id, part_no, expires
    )
    logger.info(f'Uploading part [{part_no}]...')
    logger.debug(f'[{part_no}] Presigned url {signed_url}')
    res = requests.put(signed_url, data=part)
    logger.debug(f'headers: {res.headers}')
    etag = res.headers.get('ETag')
    logger.debug(f'part: [{part_no}] Etag {etag}')
    return {
        'ETag': etag,
        'PartNumber': part_no,
    }  # you have to append etag and partnumber of each parts


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
    with open(file_name, 'rb') as rf:
        rf.seek(cursor)
        data = rf.read(chunk_size)
        if data is None or len(data) == 0:
            return None
        logger.info(f'Uploading part [{part_no}]...')
        for i in range(0, 5):
            try:
                logger.info(f'Uploading part [{part_no}] to url: {url}...')
                res = requests.put(url, data=data)
                if 'Connection' in res.headers and res.headers['Connection'] == 'close':
                    continue
                logger.debug(f'{part_no} - headers: {res.headers}')
                etag = res.headers.get('ETag', '')
                return {'ETag': etag.replace('"', ''), 'PartNumber': part_no}
            except Exception as e:
                logger.error(f'Error {e} tryies: {i}')
                time.sleep(1)

import boto3
import logging
import hashlib
import os
import requests
import time
import concurrent.futures
from s3_client_lib.s3_client import S3Client
from s3_client_lib.utils import *
import multiprocessing as mp

logger = logging.getLogger(__name__)


class S3MultipartClient(S3Client):
    def __init__(
        self,
        address,
        access_key,
        secret_access_key,
        tenant=None,
        signature_version='s3v4',
    ):
        super(S3MultipartClient, self).__init__(
            address, access_key, secret_access_key, tenant, signature_version
        )

    def upload_local_file(self, local_file, bucket, object_name):
        """

        :param local_file:
        :param object_name:
        :param size:
        :return:
        """
        # file is greater then 512 mb we have to upload it with multipart
        file_size = os.path.getsize(os.path.abspath(local_file))
        num_chunks, max_size = get_file_chunk_size(file_size)
        self.upload_local_file_multipart(
            local_file, bucket, object_name, chunk_size=max_size, num_chunks=num_chunks
        )

    def sign_s3_upload(
        self, bucket, object_name, fields=None, conditions=None, expires=3600
    ) -> dict:
        """
        Create presigned url for upload of object to S3. This method is for smaller files.

        :param object_name: The name of the bucket to presign the post to
        :param fields:  A  dictionary  of  prefilled  form  fields  to  build  on  top  of.   Elements  that  may  be
                        included  are  acl,  Cache-Control,  Content-Type,  Content-Disposition, Content-Encoding,
                        Expires, success_action_redirect, redirect, success_action_status, and x-amz-meta-.
                        Note that if a particular element is included in the fields dictionary it will not
                        be automatically added to the conditions list.  You must specify a condition
                        for the element as well.
        :param conditions: A list of conditions to include in the policy.
                        Each element can be either a list or a structure.
                        For example:[{“acl”:  “public-read”}, [”content-length-range”, 2, 5],
                            [”starts-with”, “$success_action_redirect”, “”]]
                        Conditions that are included may pertain to acl,  content-length-range,  Cache-Control,
                        Content-Type,  Content-Disposition,  Content-Encoding,  Expires,  success_action_redirect,
                        redirect, success_action_status, and/or x-amz-meta-.Note that if you include a condition,
                        you must specify the a valid value in the fields dictionary as well.
                        A value will not be added automatically to the fields dictionary based on the conditions.
        :param expires: The number of seconds the presigned post is valid for.
        :return: {
            "data": {...},
            "url": string
        }
        """
        return self.signed_s3_multipart_upload(bucket, object_name)

    def signed_s3_multipart_upload(
        self, bucket, object_name, size, checksum_update, origin, finish_url
    ) -> dict:
        self.create_bucket_if_not_exists(bucket)
        upload_id = self.create_multipart_upload(bucket, object_name)
        max_parts, chunk_size = get_file_chunk_size(size)
        logger.debug(f'max parts {max_parts}, chunk size: {chunk_size}')
        parts = self.create_presigned_urls_for_multipart_upload(
            bucket, object_name, upload_id, max_parts, expires
        )
        return {
            'parts_url': parts,
            'chunk_size': chunk_size,
            'checksum_update': checksum_update,
            'upload_id': upload_id,
            'origin': origin,
            'num_chunks': max_parts,
            'finish_url': f'{finish_url}/',
        }

    def create_multipart_upload(self, bucket, object_name, expires=3600):
        """
        This method will prepare S3 for multipart upload
        :param bucket: path to local file
        :param object_name: how should be object named in S3
        :param expires: when this multipart upload will expires
        :return: UploadId == UUID
        """
        res = self.client.create_multipart_upload(
            Bucket=bucket, Key=object_name, Expires=expires
        )
        return res['UploadId']

    def finish_multipart_upload(self, bucket, object_name, parts, upload_id):
        """
        End multipart upload
        :param bucket: path to local file
        :param object_name: how should be object named in S3
        :param parts: all uploaded parts
        :param: UploadId == UUID
        """
        try:
            return self.client.complete_multipart_upload(
                Bucket=bucket,
                Key=object_name,
                MultipartUpload={'Parts': parts},
                UploadId=upload_id,
            )
        except Exception as e:
            logger.error(f'finish exc {e}')

    def create_presigned_urls_for_multipart_upload(
        self, bucket, object_name, upload_id, max_part, expires=3600
    ):
        """
        Create for each part presigned url, so upload can be done in parallel.
        :param bucket: path to local file
        :param object_name: how should be object named in S3
        :param upload_id: UploadId == UUID
        :param max_part: how many parts will be created
        :return: [presignedUrl1, ..., presignedUrlN]
        """
        logger.debug(f'{object_name} - {upload_id} - {max_part}')
        return [
            create_presigned_upload_part(
                self.client, bucket, object_name, upload_id, num, expires
            )
            for num in range(1, max_part + 1)
        ]

    def upload_local_file_multipart(
        self, local_file, bucket, object_name, chunk_size=MB_512, num_chunks=1, expires=3600
    ):
        """
        Upload file to s3 with multipart upload this method is for large files
        :param local_file: path to local file
        :param object_name: how should be object named in S3
        :param chunk_size: size of chunk which will be uploaded
        :return:
        """
        upload_id = self.create_multipart_upload(bucket, object_name)
        presigned_urls = self.create_presigned_urls_for_multipart_upload(
            bucket, object_name, upload_id, num_chunks, expires
        )
        parts = []
        filename = os.path.basename(local_file)
        pool = mp.Pool(mp.cpu_count())
        futures = []
        for i in range(0, num_chunks):
            url = presigned_urls[i]
            part_no = i + 1
            cursor = i * chunk_size
            futures.append(
                pool.apply_async(
                    upload_part, args=[url, local_file, cursor, part_no, chunk_size]
                )
            )
        pool.close()
        pool.join()
        for fut in futures:
            part = fut.get()
            if part is not None:
                parts.append(part)

        logger.info('Completing upload multipart...')
        # After completing for all parts, you will use complete_multipart_upload api which requires that parts list
        self.finish_multipart_upload(bucket, object_name, parts, upload_id)
        self.finish_file_metadata(bucket, object_name, filename)

        return 'Upload completed'

    def abort_multipart_upload(self, bucket, object_name, upload_id):
        """
        This method will prepare S3 for multipart upload
        :param bucket: path to local file
        :param object_name: how should be object named in S3
        :param upload_id: UploadId == UUID
        :return: UploadId == UUID
        """
        return self.client.abort_multipart_upload(
            Bucket=bucket, Key=object_name, UploadId=upload_id
        )

import boto3
import logging
import hashlib
import os
import requests
import time
import concurrent.futures
from s3_client_lib.utils import *
from botocore.client import Config

logger = logging.getLogger(__name__)
import re


class S3Client:
    def __init__(
        self,
        address,
        access_key,
        secret_access_key,
        tenant=None,
        signature_version='s3v4',
    ):
        self.address = address
        config = Config(signature_version=signature_version)
        self.client = boto3.client(
            's3',
            endpoint_url=address,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access_key,
            config=config,
        )

        def extend_bucket_with_tenant(
            request,
            signing_name,
            region_name,
            signature_version,
            request_signer,
            operation_name,
            **kwargs,
        ):
            if tenant:
                bucket = request.context['signing'].get('bucket', None)
                logger.info(f' {tenant} => {bucket}')
                if request.url:
                    request.url = request.url.replace(bucket, f'{tenant}%3A{bucket}', 1)
            else:
                logger.info('TENANT IS NOT DEFINED => do nothing')

        self.client.meta.events.register(
            'before-sign.s3.UploadPart', extend_bucket_with_tenant
        )
        self.resource = boto3.resource(
            's3',
            endpoint_url=address,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access_key,
        )

    def finish_file_metadata(self, bucket, object_name, filename):
        s3_object = self.get_object_head(bucket, object_name)
        logger.debug(f'Fetched metadata header {s3_object}')
        self.update_metadata_object(
            bucket,
            object_name,
            {
                'contentLength': str(s3_object['ContentLength']),
                'contentType': s3_object['ContentType'],
                'contentName': filename,
            },
        )

    def create_bucket_if_not_exists(self, bucket_name, **kwargs):
        """
        Creates bucket in S3
        :param bucket_name:
        :param kwargs: {
            "ACL": 'private'|'public-read'|'public-read-write'|'authenticated-read',
            "Bucket": 'string',
            "CreateBucketConfiguration": {
                'LocationConstraint': 'EU'|'eu-west-1'|'us-west-1'|'us-west-2'|'ap-south-1'|'ap-southeast-1'|'ap-southeast-2'|'ap-northeast-1'|'sa-east-1'|'cn-north-1'|'eu-central-1'
            },
            "GrantFullControl"='string',
            "GrantRead"='string',
            "GrantReadACP"='string',
            "GrantWrite"='string',
            "GrantWriteACP"='string',
            "ObjectLockEnabledForBucket"=True|False
            }
        :return: dict
        """
        bucket = self.resource.Bucket(bucket_name)

        if bucket.creation_date:
            logger.info(f'The bucket exists {bucket_name}')
        else:
            logger.info(f'The bucket does not exist {bucket_name}')
            return self.client.create_bucket(Bucket=bucket_name, **kwargs)

    def upload_local_file(self, local_file, bucket, object_name):
        """

        :param local_file:
        :param object_name:
        :param size:
        :return:
        """
        # file is greater then 512 mb we have to upload it with multipart
        file_size = os.path.getsize(os.path.abspath(local_file))
        if file_size > GB_1:
            raise Exception('File size is greater than 1GB use S3MultipartClient')
        response_api = self.sign_s3_upload(bucket, object_name)
        s3_request_data = response_api['data']['fields']
        url = response_api['data']['url']
        filename = os.path.basename(local_file)
        with open(local_file, 'rb') as rf:
            logger.debug(f'Presigned upload => url: {url} fields: {s3_request_data}')
            s3_response = requests.post(
                url, data=s3_request_data, files={'file': (filename, rf)}
            )
            logger.debug(f'Response {s3_response} {s3_response.text}')
            self.finish_file_metadata(bucket, object_name, filename)
            return 'Upload completed'

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
        # object_name += "-${filename}"
        presigned_post = self.client.generate_presigned_post(
            Bucket=bucket,
            Key=object_name,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=expires,
        )
        url = os.path.join(os.path.join(self.address, bucket), object_name)
        return {'data': presigned_post, 'url': f'{url}'}

    def sign_s3_download(self, bucket, object_name, filename, expires=3600) -> dict:
        """
        Pre-signed url for download of file.
        :param bucket: The name of the bucket where is object
        :param object_name: The name of the object to presign
        :param filename: How should file named after download
        :param expires: The  number  of  seconds  the  presigned  url  is  valid  for.
                    By default it expires in an hour (3600 seconds)
        :return:{
            "url": string
        }
        """

        logger.debug(f'Signing url for {object_name} in bucket {bucket}')
        logger.info(f'Signing url for {object_name} in bucket {bucket}')

        presigned_url = self.client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': bucket,
                'Key': object_name,
                'ResponseContentDisposition': f'attachment; attachment; filename={filename}',
            },
            ExpiresIn=expires,
        )
        return {'url': presigned_url}

    def copy_from_s3(
        self, bucket, object_name, destination_path, chunk_size=CHUNK_SIZE_16M
    ):
        """
        Function will copy data from s3 to destination_path. For download is used get_object function which is dict which
        contains in Body key StreamingBody -> loading by chunks.

        :param object_name: The name of the object
        :param destination_path: destination where should object be downloaded
        :param chunk_size:
        :return:
        """

        logger.info(
            f'Copy data from S3 for: {object_name} from bucket: {bucket} to path: {destination_path}'
        )
        response = self.client.get_object(Bucket=bucket, Key=object_name)

        digest = hashlib.sha256()
        try:
            with open(destination_path, 'wb') as wf:
                for chunk in response['Body'].iter_chunks(chunk_size):
                    if chunk is None or not any(chunk):
                        break
                    digest.update(chunk)
                    wf.write(chunk)
            checksum_result = digest.hexdigest()
            logger.info(f'Checksum result: {checksum_result}')
            return checksum_result
        except Exception as e:
            logger.error(
                f'Something goes wrong in copy from S3 response: {response}, '
                f'destination = {destination_path}, '
                f'object_name: {object_name}'
                f'bucket: {bucket}'
                f'error: {e}'
            )

    def copy_to_s3(self, bucket, from_path, object_name, extra_args=None):
        """
        Copy file from local storage to S3.
        :param from_path: path to file
        :param object_name: how should object name
        :param extra_args: dict data which will be bind in object
        :return:
        """

        logger.info(
            f'Copy data to S3 for: {object_name} to bucket: {bucket} from path: {from_path}'
        )
        bucket = self.resource.Bucket(bucket)
        return bucket.upload_file(from_path, object_name, ExtraArgs=extra_args)

    def copy_from_bucket_to_bucket(
        self, object_name, source_bucket, destination_bucket
    ):
        """
        Copy objects from one bucket to another
        :param object_name: The name of the object to copy
        :param source_bucket:
        :param destination_bucket:
        :return:
        """
        copy_source = {'Bucket': source_bucket, 'Key': object_name}
        logger.info(
            f'Copy data between buckets S3 for: {object_name} source bucket: {source_bucket} '
            f'destination bucket: {destination_bucket}'
        )
        to_bucket = self.resource.Bucket(destination_bucket)
        return to_bucket.copy(copy_source, object_name)

    @staticmethod
    def __paginate(bucket, marker, size, iterator):
        result = []
        page = None

        while True:
            response_iterator = iterator(bucket, marker)

            for page in response_iterator:
                logger.debug(page)
                result.append(page)
            if page is None or page['Marker'] == '':
                return page, result
            if size is not None:
                if len(result) > size:
                    return page['Marker'], result
            try:
                previous = marker
                marker = page['Marker']
            except KeyError:
                break
            if previous == marker and len(previous) > 0 and len(marker) > 0:
                break

    def list_objects(
        self, bucket=None, prefix=None, max_items=100, start_from=None, size=100
    ):
        """
        List objects from bucket

        :param prefix: filter objects which starts with
        :param max_items: max items on page
        :param start_from: marker from which will be data fetched
        :param size: max size of result data is is None return all data
        :return: marker, [objects]
        """
        config = {'PageSize': max_items}
        if prefix is not None:
            config.update({'Prefix': prefix})
        marker = start_from
        paginator = self.client.get_paginator('list_objects')

        def iterate(bucket, mark):
            config.update({'StartingToken': mark})
            return paginator.paginate(Bucket=bucket, PaginationConfig=config)

        return self.__paginate(bucket, marker, size, iterate)

    def search_objects(self, bucket, query, start_from=None, max_items=100, size=100):
        """
        Search by query in https://jmespath.org/ format.
        :param query:
        :param start_from: marker from start
        :param max_items: max items on page
        :param size: max size of result data is is None return all data
        :return: marker, [objects]
        """
        config = {'PageSize': max_items}
        marker = start_from
        paginator = self.client.get_paginator('list_objects')

        def iterate(bucket, mark):
            config.update({'StartingToken': mark})
            page_iterator = paginator.paginate(Bucket=bucket, PaginationConfig=config)
            return page_iterator.search(query)

        return self.__paginate(bucket, marker, size, iterate)

    def list_buckets(self):
        """
        List all buckets from S3
        :return:
        """
        return self.client.list_buckets()

    def delete_object(self, bucket, object_name):

        logger.debug(f'Deleting object {object_name} from bucket {bucket}')
        response = self.resource.Object(bucket, object_name).delete()
        return response

    def update_metadata_object(self, bucket, object_name, metadata):
        logger.debug(
            f'Update metadata on object {object_name} in bucket: {bucket} With metdata: {metadata}'
        )
        s3_object = self.resource.Object(bucket, object_name)
        s3_object.metadata.update(metadata)
        result = s3_object.copy_from(
            CopySource={'Bucket': bucket, 'Key': object_name},
            Metadata=s3_object.metadata,
            MetadataDirective='REPLACE',
        )
        return result

    def get_object_head(self, bucket, object_name):
        """
        Get mostly metadatada about object.
        :param object_name:
        :return:
        """

        response = self.client.head_object(Bucket=bucket, Key=object_name)
        return response

    def get_object(self, bucket, object_name):
        """
        Get whole object
        :param object_name:
        :return:
        """
        response = self.client.get_object(Bucket=bucket, Key=object_name)
        return response

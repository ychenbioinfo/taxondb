# =============================================================================
# Confidential and Proprietary
# Unauthorized copying of this file via any medium is strictly prohibited
# Copyright (C) Aperiomics, Inc., 2019
# Written by Alvin Chen <ychen@aperiomics.com>
# ==============================================================================
import urllib
import tarfile
import os
from io import BytesIO, StringIO
from tempfile import mkstemp

import boto3

s3 = boto3.resource('s3')


class S3File:
    """
    Helper class for Sqlite file AWS S3 storage.
    Temporary local file will be used as catch for database read and write.

    Environment Variables:
        AWS_ACCESS_KEY_ID
            The access key for your AWS account.
        AWS_SECRET_ACCESS_KEY
            The secret key for your AWS account.

    Args:
        s3_file: S3 key for the database file.
        is_new_db: whether the database file is already exists. If the file is already
                  exists, temporary file will copy from S3 target first.
        s3_bucket: S3 bucket name. If not provided, it will use environment variable
                   AWS_STORAGE_BUCKET_NAME

    Attributes:
        file (str): full path for local temporary file.
        is_save (boolean): label `True` if the file has been changed in local. The updated file
                           will be saved to S3 bucket when the object destroyed.
    """

    def __init__(self, s3_file: str, is_new_db: bool = False, s3_bucket: str = ''):
        self._s3_bucket = s3_bucket

        if not s3_bucket:
            bucket_name = os.getenv('AWS_STORAGE_BUCKET_NAME', None)
            if bucket_name:
                self._s3_bucket = bucket_name
            else:
                raise ValueError('No s3 bucket name is not provided.')

        self._s3_file = s3_file
        self._is_new_db = is_new_db
        self._is_cleaned = False
        self._is_saved = True
        handle, self.file = mkstemp()
        self.is_closed = False

        if is_new_db:
            self._is_saved = False

        if not is_new_db:
            s3.meta.client.download_file(self._s3_bucket, self._s3_file, self.file)

    def __del__(self):
        """
        Save local file to S3 bucket when there is changes (is_save == True) and remove temp file.
        """
        if not self.is_closed:
            self.close()

    @property
    def is_saved(self):
        return self._is_saved

    @is_saved.setter
    def is_saved(self, val: bool):
        self._is_saved = val

    def close(self):
        """
        Close the file writing process and upload file to online storage.
        """
        if not self._is_saved:
            s3.meta.client.upload_file(self.file, self._s3_bucket, self._s3_file)
        self.clean()
        self.is_closed = True
        self._is_saved = True

    def terminate(self):
        """
        Terminate the writing process, clean temporary file but don't save file to online storage.
        """
        self.clean()
        self.is_closed = True

    def write(self, data: bytes):
        """
        Write content directly to the local temp file.

        Args:
            data: content write to the local file.
        """
        with open(self.file, 'ab') as fh:
            fh.write(data)
        self._is_saved = False

    def clean(self):
        """
        Remove local temporary file.
        """
        try:
            os.remove(self.file)
            self._is_cleaned = True
        except:
            pass

    def delete(self):
        """
        Delete file object from S3, and delete local storage

        Return:
        """
        if not self._is_new_db:
            s3.Object(self._s3_bucket, self._s3_file).delete()
            self.clean()
            return True

    def save(self):
        if not self._is_saved:
            s3.meta.client.upload_file(self.file, self._s3_bucket, self._s3_file)
            self._is_saved = True


def download_file(url, timeout=20, retry=0):
    """
    Download file from the given url

    :param url: str: url for the target file
    :param timeout: int: waiting time for download time
    :param retry: int: number of time retry after connection timeout
    :return: BytesIO: bytesIO of file content
    """

    def down_task(url, timeout):
        req = urllib.request.Request(url)
        data = urllib.request.urlopen(req, timeout=timeout)
        buf = BytesIO(data.read())
        return buf

    count = 0

    while count <= retry:
        try:
            res = down_task(url, timeout)
            return res
        except socket.timeout:
            continue
        except:
            raise

        count += 1

    raise ConnectionError('Connection Timeout, exit after tried %s times' % count)


def extract_file_from_tar(content, target_fn, out_type='BytesIO'):
    """
    extract target file from tar content

    :param content: BytesIO, str: file name or BytesIO content
    :param target_fn: str: target file name
    :param out_type: str: output type of the result
    :return: value type based on out_type
    """

    if not isinstance(content, BytesIO) and not isinstance(content, str):
        raise TypeError("Unsupported data type")

    support_types = ['bytes', 'str', 'StringIO', 'BytesIO']
    if out_type not in support_types:
        raise TypeError("Unsupported output data type: ", out_type)

    if isinstance(content, BytesIO):
        tar_data = tarfile.open(fileobj=content)
    else:
        tar_data = tarfile.open(content)

    if target_fn not in tar_data.getnames():
        raise KeyError("Cannot find target %s in tar file" % target_fn)

    tar_info = tar_data.getmember(target_fn)
    tar_handle = tar_data.extractfile(tar_info)
    if out_type == 'str':
        content = tar_handle.read().decode("utf-8")
    elif out_type == 'bytes':
        content = tar_handle.read()
    elif out_type == 'StringIO':
        content = StringIO(tar_handle.read().decode('utf-8'))
    else:
        content = BytesIO(tar_handle.read())

    tar_data.close()
    return content

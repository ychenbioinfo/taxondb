# =============================================================================
# Confidential and Proprietary
# Unauthorized copying of this file via any medium is strictly prohibited
# Copyright (C) Aperiomics, Inc., 2019
# Written by Alvin Chen <ychen@aperiomics.com>
# ==============================================================================
import os
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
            self._is_saved = True

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

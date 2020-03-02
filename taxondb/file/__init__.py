# =============================================================================
# Confidential and Proprietary
# Unauthorized copying of this file via any medium is strictly prohibited
# Copyright (C) Aperiomics, Inc., 2019
# Written by Alvin Chen <ychen@aperiomics.com>
# ==============================================================================

import urllib
import tarfile
from io import BytesIO, StringIO

from .s3_file import S3File


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

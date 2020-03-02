# =============================================================================
# Confidential and Proprietary
# Unauthorized copying of this file via any medium is strictly prohibited
# Copyright (C) Aperiomics, Inc., 2019
# Written by Alvin Chen <ychen@aperiomics.com>
# ==============================================================================

import uuid
from taxondb.file import S3File


def test_create_newfile():
    file_name = uuid.uuid4().hex[:12] + '.txt'
    s3_create_file = S3File(file_name, is_new_db=True, s3_bucket='aperiomics-xploredb-dev')
    s3_create_file.write(b'test\n')
    s3_create_file.close()

    s3_retrieve_file = S3File(file_name, s3_bucket='aperiomics-xploredb-dev')
    s3_retrieve_file.delete()




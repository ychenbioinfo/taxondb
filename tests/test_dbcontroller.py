# =============================================================================
# Confidential and Proprietary
# Unauthorized copying of this file via any medium is strictly prohibited
# Copyright (C) Aperiomics, Inc., 2019
# Written by Alvin Chen <ychen@aperiomics.com>
# ==============================================================================

import os
from taxondb import DBController
from taxondb.file import S3File

current_dir = os.path.dirname(__file__)


def test_dbcontroller():
    controller1 = DBController()
    controller1.connect(os.path.join(current_dir, 'data/testdb.sqlite'))

    controller2 = DBController()
    controller2.connect('test/newdb.sqlite', is_new_db=True, is_s3=True, s3_bucket='aperiomics-xploredb-dev')
    controller2.copy_table(controller1.db_connector, 'test')

    s3_retrieve_file = S3File('test/newdb.sqlite', s3_bucket='aperiomics-xploredb-dev')
    s3_retrieve_file.delete()




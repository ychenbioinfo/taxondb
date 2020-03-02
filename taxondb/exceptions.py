# =============================================================================
# Confidential and Proprietary
# Unauthorized copying of this file via any medium is strictly prohibited
# Copyright (C) Aperiomics, Inc., 2019
# Written by Alvin Chen <ychen@aperiomics.com>
# ==============================================================================


class DBConfigureTypeError(TypeError):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


class DBConfigureError(ValueError):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


class DBConnectionError(ConnectionError):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


class DBRecordNotFoundError(IndexError):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


class TaxonomyDataError(LookupError):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


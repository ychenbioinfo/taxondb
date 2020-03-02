# =============================================================================
# Confidential and Proprietary
# Unauthorized copying of this file via any medium is strictly prohibited
# Copyright (C) Aperiomics, Inc., 2019
# Written by Alvin Chen <ychen@aperiomics.com>
# ==============================================================================

import os
import logging
import pip

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from . import exceptions

Base = declarative_base()


class DBConfigure:

    TYPES = ['sqlite', 'mysql', 'postgresql']

    def __init__(self):
        self._type = ""
        self._dialect = ""
        self._address = ""
        self._port = 0
        self.name = ""
        self.user = ""
        self.password = ""
        self.path = ""
        self.encoding = ""

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, value):
        if value not in self.TYPES:
            raise exceptions.DBConfigureTypeError("Unsupported database type %s" % self.type)
        self._type = value

    @property
    def port(self):
        return self._port

    @port.setter
    def port(self, value: int):
        self._port = value

    @property
    def address(self):
        return self._address

    @address.setter
    def address(self, value):
        self._address = value

    @property
    def dialect(self):
        return self._dialect

    @dialect.setter
    def dialect(self, value):
        installed_packages = [x.key for x in pip.get_installed_distributions()]
        if value not in installed_packages:
            raise exceptions.DBConfigureError("Database dialect %s has not installed" % value)
        self._dialect = value

    def __str__(self):
        str_out = self.type
        if self.type == 'sqlite':
            str_out += " " + self.path
        else:
            str_out += " " + self.address + ":" + str(self.port) + " " + self.name
        return str_out

    def check_configs(self):

        if self.type == 'sqlite':
            if self.path == "":
                raise exceptions.DBConfigureError("db_path is required for %s" % self.type)
        else:
            if self.address == "":
                raise exceptions.DBConfigureError("db_addr is required for %s" % self.type)
            if self.name == "":
                raise exceptions.DBConfigureError("db_name is required for %s" % self.type)
            if self.user == "":
                raise exceptions.DBConfigureError("db_user is required for %s" % self.type)
            if self.port == 0:
                raise exceptions.DBConfigureError("db_port is required for %s" % self.type)
        return True

    def get_conn_url(self):

        if self.type == 'sqlite':
            if not os.path.isfile(self.path):
                raise exceptions.DBConfigureError("Cannot find database file %s" % self.path)

            if os.path.isabs(self.path):
                conn_url = 'sqlite:////%s' % self.path
            else:
                conn_url = 'sqlite:///%s' % self.path

        else:
            conn_url = self.type
            if self.dialect != "":
                conn_url += '+' + self.dialect
            conn_url = "%s://%s:%s@%s:%s/%s" % (
                conn_url,
                self.user,
                self.password,
                self.address,
                self.port,
                self.name
            )
        return conn_url


class DBConnector:

    def __init__(self):
        self._engine = None
        self._Session = None
        self._session = None
        self._db_config = None
        self._is_connected = False

    def __del__(self):
        self.close()

    def get_engine(self):
        if self._is_connected:
            return self._engine
        else:
            raise exceptions.DBConnectionError('Database has not been connected yet.')

    @property
    def session(self):
        if self._is_connected:
            return self._session
        else:
            raise exceptions.DBConnectionError('Database has not been connected yet.')

    def connect(self, db_config: DBConfigure):
        self._db_config = db_config
        try:
            self._db_config.check_configs()
        except Exception as e:
            logging.error(e)
            return False

        conn_url = self._db_config.get_conn_url()

        self._engine = create_engine(conn_url)
        self._Session = sessionmaker(bind=self._engine)
        self._session = self._Session()

        self._is_connected = True
        return True

    def is_connected(self):
        return self._is_connected

    def get_new_session(self):
        return self._Session()

    def create_table(self, table_class, overwrite=False):
        if not overwrite:
            if self.get_engine().dialect.has_table(self.get_engine(), table_class.__tablename__):
                logging.warning("Table %s already exists. Use 'overwrite=True' to create new table."
                                % table_class.__tablename__)
                return False
        self.get_engine().execute("DROP TABLE IF EXISTS %s" % table_class.__tablename__)
        Base.metadata.tables[table_class.__tablename__].create(bind=self._engine)
        logging.debug("Create table %s" % table_class.__tablename__)
        return True

    def close(self):
        self._session.close()
        self._engine.dispose()

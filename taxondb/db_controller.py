# =============================================================================
# Confidential and Proprietary
# Unauthorized copying of this file via any medium is strictly prohibited
# Copyright (C) Aperiomics, Inc., 2019
# Written by Alvin Chen <ychen@aperiomics.com>
# ==============================================================================
import logging

import os
import collections
import sqlalchemy as sa
import pandas as pd

from .db_connector import DBConnector, DBConfigure
from . import exceptions
from .file import S3File, download_file, extract_file_from_tar
from .models import TaxonNodes, TaxonNames


class SqliteDBController(object):
    """
    Base class for database connection and control
    """

    def __init__(self):
        self.db_config = None
        self.db_connector = None
        self._is_new_db = False
        self._is_s3 = False
        self._s3_file = None
        self._s3_bucket = ''
        self._file_path = ''

    def __str__(self):
        out_str = '<' + type(self).__name__
        if self.db_config is not None:
            out_str += ": " + self.db_config.get_conn_url()
        out_str += '>'

        return out_str

    @property
    def db_path(self):
        """Get file path of the database. If it is linked to s3 file, it will return the path of the
        temporary local storage path of the database file.

        Return: string of the file path
        """
        if self._is_s3:
            return self._s3_file.file
        else:
            return self._file_path

    def connect(self, file_path: str, is_new_db: bool = False, is_s3: bool = False,
                s3_bucket: str = ''):
        """Connect to sqlite database

        Args:
            file_path: path of the database file, it can also be the key of S3 file.
            is_new_db: if connects to the existing s3 db file, the file will be synced to local
                temporary file system.
            is_s3: if the database file is local or on S3
            s3_bucket: S3 bucket name. If not provided, it will use environment variable
                AWS_STORAGE_BUCKET_NAME. Required when `is_s3=True`
        """
        self._is_new_db = is_new_db
        self._is_s3 = is_s3
        self._s3_bucket = s3_bucket
        self._file_path = file_path

        if is_s3:
            self._s3_file = S3File(s3_file=file_path, is_new_db=is_new_db, s3_bucket=s3_bucket)
        else:
            if is_new_db:
                if not os.path.isfile(self._file_path):
                    with open(self._file_path, 'w'):
                        pass

        self.db_config = DBConfigure()
        self.db_config.type = 'sqlite'
        self.db_config.path = self.db_path
        self.db_connector = DBConnector()
        self.db_connector.connect(self.db_config)

        return True

    def is_connected(self):
        if self.db_connector is None:
            return False
        else:
            return self.db_connector.is_connected

    def copy_table(self, source_db_connector: DBConnector, table_name):

        if not self.is_connected():
            raise exceptions.DBConnectionError("Target database has not been connected!")

        if not source_db_connector.is_connected:
            raise exceptions.DBConnectionError("Source database has not been connected!")

        meta = sa.MetaData(source_db_connector.get_engine())
        table = sa.Table(table_name, meta, autoload=True)

        self.db_connector.get_engine().execute("DROP TABLE IF EXISTS " + table_name)
        table.create(self.db_connector.get_engine())

        rows = source_db_connector.get_engine().execute(table.select()).fetchall()
        with self.db_connector.get_engine().begin() as con:
            con.execute(table.insert(), rows)

        return True

    def close(self):
        self.db_connector.close()
        if self._is_s3:
            self._s3_file.close()


class TaxonomyDBCreator(SqliteDBController):
    """
    Class to create new taxonomy database
    """

    taxon_file = "ftp://ftp.ncbi.nih.gov/pub/taxonomy/taxdump.tar.gz"
    nodes_columns = ["tax_id", "parent_tax_id", "rank", "embl_code", "division_id",
                     "inherited_div_flg", "genetic_code_id", "inherited_GC_flag",
                     "mito_genetic_code_id", "inherited_MGC_flag", "GenBank_hidden_flag",
                     "hidden_subtree_root_flag", "comments"]
    names_columns = ["tax_id", "name_txt", "unique_name", "name_class"]
    names_file = "names.dmp"
    nodes_file = "nodes.dmp"

    def __init__(self):
        super().__init__()

    def create(self):
        if not self.is_connected():
            raise exceptions.DBConnectionError('Controller has not been connected yet.')

        self._create_names_data()
        self._create_nodes_data()

    def _create_nodes_data(self):
        self.db_connector.create_table(TaxonNodes)
        df_nodes = self._download_taxon_data(self.nodes_file, self.nodes_columns)
        df_nodes.drop('comments', 1, inplace=True)
        self._write_taxon_data(df_nodes, TaxonNodes)
        return True

    def _create_names_data(self):
        self.db_connector.create_table(TaxonNames)
        df_names = self._download_taxon_data(self.names_file, self.names_columns)
        df_names = df_names.loc[df_names['name_class'] == "scientific name", ]
        df_names.drop('name_class', 1, inplace=True)
        self._write_taxon_data(df_names, TaxonNames)
        return True

    def _download_taxon_data(self, filen, col_names):
        logging.debug("TaxonomyCreator: downloading taxonomy file %s from NCBI..." % filen)
        file_data = download_file(self.taxon_file)
        fh = extract_file_from_tar(file_data, filen)
        df_taxon = pd.read_csv(fh, sep=r'\t\|\t', header=None, index_col=None, engine='python')
        df_taxon.columns = col_names
        df_taxon = df_taxon.astype(object).where(pd.notnull(df_taxon), None)
        # remove "\t|" from last column cause by using sep="\t\|\t" in pd.read_csv
        df_taxon.iloc[:, -1] = [x.replace("\t|", "") for x in list(df_taxon.iloc[:, -1])]
        logging.debug("Done!")
        return df_taxon

    def _write_taxon_data(self, data_frame, table_class):
        logging.debug("TaxonomyCreator: writing taxonomy data to %s..." % table_class.__tablename__)
        if self.db_connector.create_table(table_class, overwrite=True):
            try:
                self.db_connector.get_engine().execute(table_class.__table__.insert(),
                                                       data_frame.to_dict(orient='records'))
                logging.debug("TaxonomyCreator: done!")
                return True
            except Exception as e:
                logging.error("TaxonomyCreator: DB connection error: %s" % e)
                raise e
        return False


class TaxonomyDBFinder(SqliteDBController):
    """
    Class for searching taxonomy database
    """

    default_clades = ["superkingdom", "kingdom", "phylum", "class",
                      "order", "family", "genus", "species"]

    def __init__(self):
        super().__init__()
        self.phylo_tree = None
        self.rev_phylo_tree = None
        self.phylo_rank = None

    def find_taxid_parents(self, tid: int, clades: list = []):
        """return taxonomy parents of requested taxonomy id

        Example:
            find_taxid_parents("9606")
            return:
                ({'species': 9606, 'genus': 9605, 'family': 9604, 'order': 9443, 'class': 40674,
               'phylum': 7711, 'kingdom': 33208, 'superkingdom': 2759},
                {9606: 'Homo sapiens', 9605: 'Homo', 9604: 'Hominidae', 9443: 'Primates',
                40674: 'Mammalia', 7711: 'Chordata', 33208: 'Metazoa', 2759: 'Eukaryota'})

        Args:
            tid: taxonomy id
            clades: list of taxonomy ranks that need to be included in the result

        Returns:
            Tuple of two dictionaries contain parent taxonomy ids and names
        """

        if self.rev_phylo_tree is None:
            self._build_rev_phylo_tree()

        phylo_tree = self.rev_phylo_tree
        phylo_rank = self.phylo_rank
        if len(clades) < 1:
            clades = self.default_clades

        routes = list()

        if tid not in phylo_tree:
            return None, None

        def _walk(tid):
            if tid == phylo_tree[tid]:
                return
            routes.append([tid, phylo_rank[tid]])
            _walk(phylo_tree[tid])
        _walk(tid)

        rank_tids = {}
        tid_names = {}

        for tid, rank in routes:
            if rank in clades:
                rank_tids[rank] = tid
                tid_name = self.db_connector.session.query(TaxonNames.name_txt).filter(
                    TaxonNames.tax_id == tid).first()
                tid_names[tid] = tid_name[0]

        return rank_tids, tid_names

    def find_taxid_parents_simple(self, tid: int, clades: list = []):
        """
        Work with database record directly. Less overhead, but slow on large number of queries.
        """
        if not self.is_connected():
            logging.warning('TaxonomyFinder: no database is connected!')
            return None, None

        if len(clades) == 0:
            clades = ["superkingdom", "kingdom", "phylum", "class",
                      "order", "family", "genus", "species"]
        rank_tids = {}
        tid_names = {}
        try:
            res = self.db_connector.session.query(TaxonNodes).\
                filter(TaxonNodes.tax_id == tid).first()
            if res is None:
                logging.warning("TaxonomyFinder: unknown taxonomy id: %s" % tid)
                return None, None

            if res.rank in clades:
                rank_tids[res.rank] = res.tax_id
                tid_name = self.db_connector.session.query(TaxonNames.name_txt).filter(
                    TaxonNames.tax_id == res.tax_id).first()
                tid_names[res.tax_id] = tid_name[0]

        except Exception as e:
            logging.error(e)
            return None, None

        # find all parents until child == parent (root)
        while res.tax_id != res.parent_tax_id:
            try:
                res = self.db_connector.session.query(TaxonNodes).\
                    filter(TaxonNodes.tax_id == res.parent_tax_id).first()
                if res.rank in clades:
                    rank_tids[res.rank] = res.tax_id
                    tid_name = self.db_connector.session.query(TaxonNames.name_txt).\
                        filter(TaxonNames.tax_id == res.tax_id).first()
                    tid_names[res.tax_id] = tid_name[0]

                if len(rank_tids) == len(clades):  # found all clade
                    break

            except Exception as e:
                logging.error("TaxonomyFinder: error: %s" % e)
                return None, None

        # add root information if required
        if 'root' in clades:
            rank_tids['root'] = 1
            tid_names[1] = 'root'

        return rank_tids, tid_names

    def get_db_taxonomy(self, tids: list, clades: list = [], match_input: bool = False):
        """
        Obtain phylogenetic clades of giving taxonomy ids.

        Args:
            tids: list(int): list of taxonomy ids
            clades: list(str): clade names of phylogenetic tree
            match_input: bool: add missing data into the result to match the total number of queries

        Returns:
            pd.dataframe: data frame of taxonomy information
                - rank_tids
                - tid_names
        """

        if len(clades) < 1:
            clades = self.default_clades

        taxon_names = []  # 2D array store all taxonomy names
        taxon_ids = []  # 2D array store all taxonomy ids

        query_count = 0
        logging.debug("TaxonomyFinder: total number of queried tids is %s" % len(tids))
        for tid in tids:

            query_count += 1
            # print("%s tids haven been processed!" % query_count)
            if query_count % 1000 == 0:
                logging.debug("TaxonomyFinder: %s tids have been processed!" % query_count)

            rank_tids, tid_names = self.find_taxid_parents(int(tid), clades)

            line_name = []
            line_id = []

            line_name.append(tid)
            line_id.append(tid)

            if rank_tids is None:
                if match_input:
                    line_name.extend([None] * len(clades))
                    line_id.extend([None] * len(clades))
                    taxon_names.append(line_name)
                    taxon_ids.append(line_id)
                continue

            def_name = "NA"
            def_id = 0  # for unclassified taxid
            for clade in clades:
                if clade in rank_tids:
                    line_name.append(tid_names[rank_tids[clade]])
                    line_id.append(rank_tids[clade])
                    if clade == 'superkingdom':
                        def_name = tid_names[rank_tids[clade]]
                    else:
                        def_name = "unclassified " + tid_names[rank_tids[clade]]
                else:
                    line_id.append(def_id)
                    line_name.append(def_name)
                    if clade == 'kingdom':
                        def_name = 'unclassified ' + def_name
            # print(line_name)
            taxon_names.append(line_name)
            taxon_ids.append(line_id)

        df_taxon_names = pd.DataFrame(taxon_names)
        df_taxon_ids = pd.DataFrame(taxon_ids)
        clades.insert(0, 'tid')
        df_taxon_names.columns = clades
        df_taxon_ids.columns = clades

        return df_taxon_names, df_taxon_ids

    def find_taxid_childrens(self, tid: int):
        """
        Search the tree for all the children of the given taxonomy ID

        Args:
            tid: taxonomy ID

        Return:
            set of children ids
        """

        if self.phylo_tree is None:
            self._build_phylo_tree()

        def _walk(node, values=[]):
            for key, item in list(node.items()):
                values.append(int(key))
                if isinstance(item, dict):
                    _walk(item, values)
            return set(values)

        nodes = _walk(self.phylo_tree[tid])
        return nodes

    def _build_phylo_tree(self):

        if not self.is_connected():
            logging.error('TaxonomyFinder: no database has been connected!')
            raise exceptions.DBConnectionError("No database has been connected!")

        logging.debug("Creating taxonomy phylogenetic tree...")
        phylo_tree = collections.defaultdict(dict)
        edges = self.db_connector.session.query(TaxonNodes.tax_id, TaxonNodes.parent_tax_id)
        for child, parent in edges:
            phylo_tree[parent][child] = phylo_tree[child]

        self.phylo_tree = phylo_tree

    def _build_rev_phylo_tree(self):

        if not self.is_connected():
            logging.error('TaxonomyFinder: no database has been connected!')
            raise exceptions.DBConnectionError("No database has been connected!")

        logging.debug("Creating reverse taxonomy phylogenetic tree...")
        phylo_tree = dict()
        phylo_rank = dict()

        dataset = self.db_connector.session.query(
            TaxonNodes.tax_id,
            TaxonNodes.parent_tax_id,
            TaxonNodes.rank)\
            .all()

        for rec in dataset:
            phylo_tree[rec.tax_id] = rec.parent_tax_id
            phylo_rank[rec.tax_id] = rec.rank

        self.rev_phylo_tree = phylo_tree
        self.phylo_rank = phylo_rank


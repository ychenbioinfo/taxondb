# =============================================================================
# Confidential and Proprietary
# Unauthorized copying of this file via any medium is strictly prohibited
# Copyright (C) Aperiomics, Inc., 2019
# Written by Alvin Chen <ychen@aperiomics.com>
# ==============================================================================

from sqlalchemy import Column, VARCHAR, SMALLINT, INTEGER, CHAR

from .db_connector import Base


class TaxonNodes(Base):
    __tablename__ = 'taxon_nodes'
    id = Column(INTEGER, primary_key=True)
    tax_id = Column(INTEGER, nullable=False, index=True, unique=True)
    parent_tax_id = Column(INTEGER, nullable=False, index=True)
    rank = Column(VARCHAR(16))
    embl_code = Column(VARCHAR(16))
    division_id = Column(VARCHAR(8))
    inherited_div_flag = Column(SMALLINT)
    genetic_code_id = Column(VARCHAR(2))
    inherited_GC_flag = Column(CHAR(1))
    mito_genetic_code_id = Column(VARCHAR(2))
    inherited_MGC_flag = Column(CHAR(1))
    GenBank_hidden_flag = Column(CHAR(1))
    hidden_subtree_root_flag = Column(CHAR(1))


class TaxonNames(Base):
    __tablename__ = 'taxon_names'
    id = Column(INTEGER, primary_key=True)
    tax_id = Column(INTEGER, nullable=False, index=True, unique=True)
    name_txt = Column(VARCHAR(128))
    unique_name = Column(VARCHAR(128))


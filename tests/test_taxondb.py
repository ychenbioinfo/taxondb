# =============================================================================
# Confidential and Proprietary
# Unauthorized copying of this file via any medium is strictly prohibited
# Copyright (C) Aperiomics, Inc., 2019
# Written by Alvin Chen <ychen@aperiomics.com>
# ==============================================================================

from taxondb import TaxonomyDBCreator, TaxonomyDBFinder
import os

current_dir = os.path.dirname(__file__)


def test_taxondb():

    temp_file = os.path.join(current_dir, 'taxon.sqlite')
    taxon_creator = TaxonomyDBCreator()
    taxon_creator.connect(temp_file, is_new_db=True)
    taxon_creator.create()
    taxon_creator.close()

    result = ({'species': 9606, 'genus': 9605, 'family': 9604, 'order': 9443, 'class': 40674,
               'phylum': 7711, 'kingdom': 33208, 'superkingdom': 2759},
              {9606: 'Homo sapiens', 9605: 'Homo', 9604: 'Hominidae', 9443: 'Primates',
               40674: 'Mammalia', 7711: 'Chordata', 33208: 'Metazoa', 2759: 'Eukaryota'})

    taxon_finder = TaxonomyDBFinder()
    taxon_finder.connect(temp_file)
    search_result = taxon_finder.find_taxid_parents_simple(9606)
    assert search_result == result

    assert taxon_finder.find_taxid_childrens(9605) == {741158, 1425170, 63221, 9606}

    os.unlink(temp_file)

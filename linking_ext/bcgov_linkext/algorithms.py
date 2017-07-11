import os
import sys
import logging
import pandas as pd
import numpy as np

from cdilinker.plugins.base import AlgorithmProvider
from cdilinker.linker.union_find import UnionFind

from jellyfish import levenshtein_distance, jaro_winkler

logger = logging.getLogger(__name__)


class Levenshtein(AlgorithmProvider):
    name = 'LEVENSHTEIN'
    title = 'Levenshtein'
    type = 'DTR'
    args = ['max_edits']

    def apply(self, s1, s2, max_edits=0):

        strings = pd.concat([s1, s2], axis=1, ignore_index=True)
        strings = strings.replace(np.nan, '', regex=True)

        def levenshtein_alg(x, max_edits=0):
            try:
                d = levenshtein_distance(x[0], x[1])
                return 1 if d <= max_edits else 0
            except Exception as err:
                logger.error(
                    'Error in calculating Levenshtein edit distance: {}'
                    .format(err))

        return strings.apply(levenshtein_alg, axis=1, max_edits=max_edits)


class JaroWinkler(AlgorithmProvider):
    name = 'JARO_WINKLER'
    title = 'Jaro-Winkler'
    type = 'DTR'
    args = ['threshold']

    def apply(self, s1, s2, threshold=1.0):

        strings = pd.concat([s1, s2], axis=1, ignore_index=True)

        def jaro_winkler_alg(x, threshold=1.0):
            try:
                t = jaro_winkler(x[0], x[1])
                return 1 if t >= threshold else 0
            except Exception as err:
                if pd.isnull(x[0]) or pd.isnull(x[1]):
                    return np.nan
                else:
                    raise err

        return strings.apply(jaro_winkler_alg, axis=1, threshold=threshold)


class LevenshteinSimilarity(AlgorithmProvider):
    """
    Levenshtein probabilistic algorithm for calculating the similarity score of string sequences.
    """
    name = 'LEVENSHTEIN'
    title = 'Levenshtein Similarity'
    type = 'PRB'
    args = []

    def apply(self, s1, s2):
        """
        Calculates the similarity score of corresponding entries in s1 and s2 using Levenshtein algorithm.
        Similarity score for each value is some float value between 0.0 and 1.0. The higher score is the more similar
        the strings are.
        :param s1: Pandas Series, First input sequence of strings
        :param s2: Pandas Series, Second input sequence of strings
        :return: Pandas Series containing the similarity score
        """
        strings = pd.concat([s1, s2], axis=1, ignore_index=True)

        def levenshtein_alg(x):
            try:
                d = levenshtein_distance(x[0], x[1])
                return 1 - d / np.max([len(x[0]), len(x[1])])
            except Exception as err:
                if pd.isnull(x[0]) or pd.isnull(x[1]):
                    return np.nan
                else:
                    raise err

        return strings.apply(levenshtein_alg, axis=1)


class JaroWinklerSimilarity(AlgorithmProvider):
    """
    Jaro-Winkler probabilistic algorithm for calculating the similarity score of string sequences.
    """
    name = 'JARO_WINKLER'
    title = 'Jaro-Winkler similarity'
    type = 'PRB'
    args = []

    def apply(self, s1, s2):
        """
        Calculates the similarity score of corresponding entries in s1 and s2 using Jaro-Winkler algorithm.
        Similarity score for each value is some float value between 0.0 and 1.0. The higher score is the more similar
        the strings are.
        :param s1: Pandas Series, First input sequence of strings
        :param s2: Pandas Series, Second input sequence of strings
        :return: Pandas Series containing the similarity score
        """
        strings = pd.concat([s1, s2], axis=1, ignore_index=True)

        def jaro_winkler_alg(x):

            try:
                return jaro_winkler(x[0], x[1])
            except Exception as err:
                if pd.isnull(x[0]) or pd.isnull(x[1]):
                    return np.nan
                else:
                    raise err

        return strings.apply(jaro_winkler_alg, axis=1)


class SynonymTable(AlgorithmProvider):
    """
    Creates disjoint sets of synonym names first auch that all synonym names will be in the same set.
    Then it compares two given series of names and check if the names are the same(synonyms) for every pair of
    names in s1 and s2.
    :param s1: Pandas Series, First input sequence of names
    :param s2: Pandas Series, Second input sequence of names
    :return: For each pair of names returns 1 if names are sysnonym and returns 0 otherwise.
    """
    name = 'SYNONYMS'
    title = 'Synonym Names'
    type = 'DTR'
    args = []

    synonym_file = "nicknames.csv"
    names_index = None
    name_sets = None

    @staticmethod
    def create_synonyms():
        '''
        Creates the disjoint sets of names using the given nicknames file.
        '''
        file_path = os.path.join(os.path.dirname(__file__), SynonymTable.synonym_file)
        nicknames = pd.read_csv(file_path)
        nicknames['nameA'] = map(lambda x: x.upper(), nicknames['nameA'])
        nicknames['nameB'] = map(lambda x: x.upper(), nicknames['nameB'])
        names = pd.concat([nicknames['nameA'], nicknames['nameB']]).drop_duplicates()
        names.index = list(range(len(names)))
        names_index = pd.Series(list(range(len(names))), index=names.values)
        name_set = UnionFind(len(names))

        for name_x, name_y in nicknames[['nameA', 'nameB']].values:
            name_set.union(names_index[name_x], names_index[name_y])

        SynonymTable.name_sets = name_set
        SynonymTable.names_index = names_index

    @staticmethod
    def synonym(name_x, name_y):
        '''
        Checks if name_x and name_y are synonym.
        :param name_x: First input name
        :param name_y: Second input name.
        :return: 1 if name_x and name_y are synonyms, 0 otherwise.
        '''
        if SynonymTable.name_sets is None:
            SynonymTable.create_synonyms()

        if pd.isnull(name_x):
            name_x = ''
        if pd.isnull(name_y):
            name_y = ''
        name_x = name_x.upper()
        name_y = name_y.upper()
        if name_x == '' and name_y == '':
            return 1
        if name_x not in SynonymTable.names_index or name_y not in SynonymTable.names_index:
            return 0

        return 1 if SynonymTable.name_sets.linked(SynonymTable.names_index[name_x],
                                                  SynonymTable.names_index[name_y]) else 0

    def __init__(self):
        if SynonymTable.name_sets is None:
            SynonymTable.create_synonyms()

    def apply(self, s1, s2):
        names = pd.concat([s1, s2], axis=1, ignore_index=True)

        def synonym_alg(x):
            return SynonymTable.synonym(x[0], x[1])

        return names.apply(synonym_alg, axis=1)

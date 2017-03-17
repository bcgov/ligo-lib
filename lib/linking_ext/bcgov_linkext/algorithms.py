import sys
import pandas as pd
import numpy as np
from cdilinker.plugins.base import AlgorithmProvider

from jellyfish import levenshtein_distance, jaro_winkler


def utf_encode(col):
    if sys.version_info[0] == 2:
        col = col.apply(
            lambda x: x.decode('utf8', 'strict') if type(x) == bytes else x
        )
    return col


class Levenshtein(AlgorithmProvider):
    name = 'LEVENSHTEIN'
    title = 'Levenshtein'
    type = 'DTR'
    args = ['max_edits']

    def apply(self, s1, s2, max_edits=0):

        s1 = utf_encode(s1)
        s2 = utf_encode(s2)
        strings = pd.concat([s1, s2], axis=1, ignore_index=True)

        def levenshtein_alg(x, max_edits=0):
            try:
                d = levenshtein_distance(x[0], x[1])
                return 1 if d <= max_edits else 0
            except Exception as err:
                if pd.isnull(x[0]) or pd.isnull(x[1]):
                    return np.nan
                else:
                    raise err

        return strings.apply(levenshtein_alg, axis=1, max_edits=max_edits)


class Jaro_Winkler(AlgorithmProvider):
    name = 'JARO_WINKLER'
    title = 'Jaro-Winkler'
    type = 'DTR'
    args = ['threshold']

    def apply(self, s1, s2, threshold=1.0):

        s1 = utf_encode(s1)
        s2 = utf_encode(s2)
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
        s1 = utf_encode(s1)
        s2 = utf_encode(s2)
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
        s1 = utf_encode(s1)
        s2 = utf_encode(s2)
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



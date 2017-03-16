import sys
import pandas as pd
import numpy as np

from ..plugins.base import AlgorithmProvider

from jellyfish import (
    soundex,
    nysiis
)


def get_algorithms(types=[None]):
    """
    Returns the list of provided algorithms of a given type.
    :param type: Type of the algorithms
    :return: List of available algorithms of the given type.
    """
    all_alg = [alg() for alg in AlgorithmProvider.plugins]

    algorithms = {}
    for alg in all_alg:
        if alg.type in types:
            algorithms[alg.name] = alg

    return algorithms


def utf_encode(col):
    if sys.version_info[0] == 2:
        col = col.apply(
            lambda x: x.decode('utf8', 'strict') if type(x) == bytes else x
        )
    return col


class NoEncoding(AlgorithmProvider):
    name = 'EXACT'
    title = 'Exact'
    type = 'TSF'
    args = []

    def apply(self, s):
        return s


class SoundexEncoding(AlgorithmProvider):
    name = 'SOUNDEX'
    title = 'Soundex Encoding'
    type = 'TSF'
    args = []

    def apply(self, s):
        s = utf_encode(s)
        return s.apply(lambda x: soundex(x) if pd.notnull(x) else np.nan)


class NyiisEncoding(AlgorithmProvider):
    name = 'NYSIIS'
    title = 'New York State Identification and Intelligence System'
    type = 'TSF'
    args = []

    def apply(self, s):
        s = utf_encode(s)
        return s.apply(lambda x: nysiis(x) if pd.notnull(x) else np.nan)


TRANSFORMATIONS = get_algorithms(types=['TSF'])


def apply_encoding(s, method='EXACT'):

    alg = TRANSFORMATIONS.get(method)
    return alg.apply(s)


class ExactComparsion(AlgorithmProvider):
    name = 'EXACT'
    title = 'Exact matching'
    type = None
    args = []

    def apply(self, s1, s2):
        """
        Compares corresponding values from the two input series and checks if they are the same.
        :param s1: Left input series.
        :param s2: Right input series.
        :return: For each (x,y) pair from two input series, return 1 if x == y and returns 0 otherwise.
        """
        x = pd.Series(0, index=s1.index)
        x[s1 == s2] = 1

        return x


class SoundexComparison(AlgorithmProvider):
    name = 'SOUNDEX'
    title = 'Soundex'
    type = None
    args = []

    def apply(self, s1, s2):
        """
        Compares the SOUNDEX encoding of two input series
        :param s1: left input series
        :param s2: right input series
        :return: Comparison series.
            For each pair (x,y) from two input series returns 1 if x and y have the same encoding,
            otherwise returns 0.
        """
        encoding = TRANSFORMATIONS.get('SOUNDEX')

        # Apply SOUNDEX encoding to both s1 and s2 and check if the encodings are the same
        s1 = encoding.apply(s1)
        s2 = encoding.apply(s2)
        x = pd.Series(0, index=s1.index)
        x[s1 == s2] = 1

        return x


class NYIISComparison(AlgorithmProvider):
    name = 'NYSIIS'
    title = 'New York State Identification and Intelligence System'
    type = None
    args = []

    def apply(self, s1, s2):
        """
        Compares the NYSIIS encoding of two input series
        :param s1: left input series
        :param s2: right input series
        :return: Comparison series.
            For each pair (x,y) from two input series returns 1 if x and y have the same encoding,
            otherwise returns 0.
        """
        encoding = TRANSFORMATIONS.get('NYSIIS')

        # Apply NYIIS encoding to both s1 and s2 and check if the encodings are the same
        s1 = encoding.apply(s1)
        s2 = encoding.apply(s2)
        x = pd.Series(0, index=s1.index)
        x[s1 == s2] = 1

        return x


class MatchSlice(AlgorithmProvider):
    name = 'SLICE_MATCH'
    title = 'Substring match'
    type = None
    args = ['start', 'end']

    def apply(self, s1, s2, start=0, end=0):
        """
        Compares a slice of the values in the two input series and checks if they are equal.
        The slice is indicated by the start and end index.
        :param s1: Left input series
        :param s2: Right input series
        :param start: Start index of the text slice
        :param end: End index of the text slice
        :return: For each (x,y) pair from two input series, return 1 if the two slices are the same
                and returns 0 otherwise.
        """

        s1 = s1.apply(lambda x: x[start:end] if pd.notnull(x) else np.nan)
        s2 = s2.apply(lambda x: x[start:end] if pd.notnull(x) else np.nan)
        x = pd.Series(0, index=s1.index)
        x[s1 == s2] = 1

        return x


class MatchHead(AlgorithmProvider):
    name = 'HEAD_MATCH'
    title = 'First n characters'
    type = None
    args = ['n']

    def apply(self, s1, s2, n=0):
        """
        Compares the first n characters of the entries in two input series and checks if they are equal.
        :param s1: Left input series
        :param s2: Right input series
        :param n: Comparison length
        :return: For each (x,y) pair from two input series, return 1 if their last n characters are the same
                and returns 0 otherwise.
        """

        s1 = s1.apply(lambda x: x[0:n] if pd.notnull(x) else np.nan)
        s2 = s2.apply(lambda x: x[0:n] if pd.notnull(x) else np.nan)
        x = pd.Series(0, index=s1.index)
        x[s1 == s2] = 1

        return x


class MatchTail(AlgorithmProvider):
    name = 'TAIL_MATCH'
    title = 'Last n characters'
    type = None
    args = ['n']

    def apply(self, s1, s2, n=0):
        """
        Compares the last n characters of the entries in two input series and checks if they are equal.
        :param s1: Left input series
        :param s2: Right input series
        :param n: Comparison length
        :return: For each (x,y) pair from two input series, return 1 if their last n characters are the same
                and returns 0 otherwise.
        """

        s1 = s1.apply(lambda x: x[-n:] if pd.notnull(x) else np.nan)
        s2 = s2.apply(lambda x: x[-n:] if pd.notnull(x) else np.nan)
        x = pd.Series(0, index=s1.index)
        x[s1 == s2] = 1

        return x


class FixedLength(AlgorithmProvider):
    name = 'FIXED_LEN'
    title = 'Exact string-length'
    type = None
    args = ['length']

    def apply(self, s1, s2, length=0):
        """
        Checks the length of characters of the entries in two input series and compares them with n.
        :param s1: Left input series
        :param s2: Right input series
        :param length: Characters length
        :return: For each (x,y) pair from two input series, return 1 if both x and y have length n
                and returns 0 otherwise.
        """
        s1 = s1.apply(lambda x: len(x) if pd.notnull(x) else np.nan)
        s2 = s2.apply(lambda x: len(x) if pd.notnull(x) else np.nan)
        x = pd.Series(0, index=s1.index)
        x[(s1 == s2) & (s1 == length)] = 1

        return x


class FixedValue(AlgorithmProvider):
    name = 'FIXED_VAL'
    title = 'Field Specific Value'
    type = None
    args = ['value']

    def apply(self, s1, s2, value):
        """
        Checks if both entries in s1 and s2 have the same value as the input value.
        :param s1: Left input series
        :param s2: Right input series
        :param value: Comparison value
        :return: For each (x,y) pair from two input series, return 1 if both x and y have the same value as the input value;
                returns 0 otherwise.
        """
        x = pd.Series(0, index=s1.index)
        x[(s1 == s2) & (s1 == value)] = 1

        return x



class AbsoluteDifference(AlgorithmProvider):
    name = 'ABS_DIFF'
    title = 'Absolute difference'
    type = None
    args = ['threshold']

    def apply(self, s1, s2, threshold=0):
        d = pd.Series.abs(s1 - s2)

        def fn(x, t):
            return 1 if x <= t else 0

        return d.apply(fn, args=(threshold,))


DETERMINISTIC_COMPARISONS = get_algorithms(types=['DTR', None])


def apply_comparison(s1, s2, method='EXACT', **args):
    alg = DETERMINISTIC_COMPARISONS.get(method)

    return alg.apply(s1, s2, **args)

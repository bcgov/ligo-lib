import sys
import pandas as pd
import numpy as np

from jellyfish import (
    soundex,
    nysiis,
    match_rating_codex,
    metaphone,
    levenshtein_distance,
    damerau_levenshtein_distance,
    jaro_distance,
    jaro_winkler,
    match_rating_comparison,
    hamming_distance,
)


def no_encode(s):
    return s


BLOCKING_METHODS = {
    'EXACT': no_encode,
    'SOUNDEX' : soundex,
    'NYSIIS': nysiis,
    'MRCODEX': match_rating_codex,
    'METAPHN': metaphone
}


def utf_encode(col):
    if sys.version_info[0] == 2:
        col = col.apply(
            lambda x: x.decode('utf8', 'strict') if type(x) == bytes else x
        )
    return col


def apply_encoding(col, method, encoding='utf8', decode_err='strict'):

    col = utf_encode(col)

    encoding_alg = BLOCKING_METHODS.get(method, no_encode)

    return col.apply(
        lambda s: encoding_alg(s) if pd.notnull(s) else np.nan
    )


def levenshtein_similarity(s1, s2, method='DTR', max_edits=0):

    s1 = utf_encode(s1)
    s2 = utf_encode(s2)

    conc = pd.concat([s1, s2], axis=1, ignore_index=True)

    def levenshtein_alg(x, method='DTR', max_edits=0):
        try:
            y = levenshtein_distance(x[0], x[1])
            if method == 'DTR':
                return 1 if y <= max_edits else 0
            else:
                return 1 - y / np.max([len(x[0]), len(x[1])])
        except Exception as err:
            if pd.isnull(x[0]) or pd.isnull(x[1]):
                return np.nan
            else:
                raise err

    return conc.apply(levenshtein_alg, axis=1, method=method, max_edits=max_edits)


def jaro_winkler_similarity(s1, s2, method='DTR', threshold=1.0):

    s1 = utf_encode(s1)
    s2 = utf_encode(s2)

    conc = pd.concat([s1, s2], axis=1, ignore_index=True)

    def jaro_winkler_alg(x, method='DTR', threshold=1.0):
        try:
            y = jaro_winkler(x[0], x[1])
            if method == 'DTR':
                return 1 if y >= threshold else 0
            else:
                y
        except Exception as err:
            if pd.isnull(x[0]) or pd.isnull(x[1]):
                return np.nan
            else:
                raise err

    return conc.apply(jaro_winkler_alg, axis=1, method=method, threshold=threshold)


def exact(s1, s2):
    """
    Compares corresponding values from the two input series and checks if they are the same.
    :param s1: Left input series.
    :param s2: Right input series.
    :return: For each (x,y) pair from two input series, return 1 if x == y and returns 0 otherwise.
    """
    x = pd.Series(0, index=s1.index)
    x[s1 == s2] = 1

    return x


def abs_diff(s1, s2, method='DTR', threshold=0):

    d = pd.Series.abs(s1 - s2)

    def fn(x, t):
        return 1 if x <= t else 0

    if method == 'DTR':
        return d.apply(fn, args=(threshold,))


def soundex_compare(s1, s2):
    """
    Compares the soundex encoding of two input series
    :param s1: left input series
    :param s2: right input series
    :return: Comparison series.
        For each pair (x,y) from two input series returns 1 if x and y have the same encoding,
        otherwise returns 0.
    """
    return exact(apply_encoding(s1, 'SOUNDEX'), apply_encoding(s2, 'SOUNDEX'))

def nysiis_compare(s1, s2):
    """
    Compares the NYSIIS encoding of two input series
    :param s1: left input series
    :param s2: right input series
    :return: Comparison series.
        For each pair (x,y) from two input series returns 1 if x and y have the same encoding,
        otherwise returns 0.
    """

    return exact(apply_encoding(s1, 'NYSIIS'), apply_encoding(s2, 'NYSIIS'))


LINKING_METHODS = {
    'EXACT': exact,
    'SOUNDEX': soundex_compare,
    'NYSIIS': nysiis_compare,
    'LEVENSHTEIN': levenshtein_similarity,
    'JARO_WINKLER': jaro_winkler_similarity,
    'ABS_DIFF': abs_diff
}

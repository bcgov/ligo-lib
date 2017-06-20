import numpy as np
import pandas as pd

from .base import PluginMount
from cdilinker.linker.algorithms import get_algorithms

CMP_ALGORITHMS = get_algorithms(types=['PRB', None])


class Comparator(metaclass=PluginMount):
    """
    Singleton Mount point for plugins that provide algorithms to calculate comparison weights of two given sets based
    on the filed type(i.e First name, Last name, Date of Birth or SIN fields).
    Plugins implementing this reference should provide the following properties:

    name :  The name of the algorithm(key)
    title:  A short description of the algorithm
    cmp_fn: Function that compares two series and returns similarity score for each pair of values in range [0,1]
    outcomes : A list of possible comparison outcomes.
    m : For each outcome (Agreemet, disagreement, missing, ...) provides the probability that fields satisfy
        the coutcome on a true match pair.
    u : For each outcome (Agreemet, disagreement, missing, ...) provides the probability that fields satisfy
        the coutcome on a true unmatched pair.

    """

    def compare(self, s1, s2, compare_fn, **args):
        # Lookup comparison function
        alg = CMP_ALGORITHMS.get(compare_fn)

        if alg is None:
            raise ValueError('Invalid comparison function name: {0}'.format(compare_fn))

        # Apply comparison algorithms to the given inputs
        cmp_vector = alg.apply(s1, s2, **args)

        return self.calculate_weights(s1, s2, cmp_vector, threshold=args.get('threshold', None))


class FirstNameComparator(Comparator):
    def __init__(self, left_data=None, right_data=None):
        self.name = 'first_name_comp'
        self.title = 'Firstname Comparison'
        self.outcomes = ['Agreement', 'Disagreement', 'Missing']
        self.m = self.__calc_m__(left_data, right_data)
        self.u = self.__calc_u__(left_data, right_data)
        self.u_vlaues = None

    def __calc_m__(self, s1=None, s2=None):
        m_vals = {'Agreement': 0.839, 'Disagreement': 0.158, 'Missing': 0.003}
        return m_vals

    def __calc_u__(self, left_data=None, right_data=None):
        u_vals = {'Agreement': 0.006, 'Disagreement': 0.989, 'Missing': 0.005}

        if left_data is None or right_data is None or left_data.empty or right_data.empty:
            return u_vals

        """
            Calculate u probability per value using the given inputs.
            1) Get the left and right field names. Assuming that left and right data frame only contain
               the single column used in comparison.

            2) Get total number of non-nan values in left and right data frames :
                NA : Number of not empty cells in left data
                NB : Number of not empty cells in right data

            3) Calculate the frequency of each value for left and right data.
                For each value x :
                    FA(x) : Number of cells with value x in left data column
                    FB(x) : Number of cells with value x in right data column

            4) Calculate u probability per value :
                u(x) = (FA(x) * FB(x) ) / (NA * NB)

        """

        left_field = left_data.columns.values[0]
        right_field = right_data.columns.values[0]

        left_len = left_data[left_field].count()  # NA
        right_len = right_data[right_field].count()  # NB

        left_count = pd.DataFrame(left_data.groupby(left_field).size())
        left_count.columns = ['left_count']

        right_count = pd.DataFrame(right_data.groupby(right_field).size())
        right_count.columns = ['right_count']

        data_index = left_count.index & right_count.index
        data = pd.DataFrame(index=data_index)
        data.index.name = 'Value'
        data = pd.concat([data, left_count, right_count], axis=1, join_axes=[data.index])
        data['u'] = (data['left_count'] * data['right_count']) / (left_len * right_len)

        self.u_vlaues = data

        """
            Calculating general case u for each outcome.
        """
        left_size = len(left_data.index.values)
        right_size = len(right_data.index.vlaues)

        m_a = np.count_nonzero(left_data[left_field].isnull().values.ravel())
        m_b = np.count_nonzero(right_data[right_field].isnull().values.ravel())

        sum = (data['left_count'] * data['right_count']).sum()

        u_vals['Agreement'] = (sum * 1.0) / (left_size * right_size)
        u_vals['Disagreement'] = (left_len * right_len - sum * 1.0) / (left_size * right_size)

        u_vals['Missing'] = (m_a * right_size + m_b * left_size - m_a * m_b) * 1.0 / (left_size * right_size)

        data.drop('left_count', axis=1, inplace=True)
        data.drop('right_count', axis=1, inplace=True)

    def calculate_weights(self, s1, s2, cmp_vector, threshold=1.0):
        # Map the comparison similarity score to comparison outcomes
        cmp_outcome = pd.Series(
            np.where(
                s1.isnull() | s2.isnull(), 'Missing',
                np.where(cmp_vector >= threshold, 'Agreement', 'Disagreement')
            )
        )

        # Get m and u probability for the outcome of each row
        weights = pd.DataFrame(index=cmp_outcome.index)
        weights['m'] = cmp_outcome.apply(lambda x: self.m[x])
        weights['u'] = cmp_outcome.apply(lambda x: self.u[x])

        # Calculate and return the comparison weight for each comparison pair
        return weights.apply(lambda x: np.log2(x['m'] / x['u']), axis=1)

import json
import logging
import numpy as np

from abc import ABCMeta, abstractmethod
from linker.linker_core.algorithms import apply_encoding, apply_comparison

logger = logging.getLogger(__name__)


class LinkBase(metaclass=ABCMeta):

    id = 0

    @classmethod
    def get_next_id(cls):
        cls.id += 1
        return cls.id

    @classmethod
    def reset_id(cls):
        cls.id = 0

    @abstractmethod
    def load_data(self):
        raise NotImplementedError('Abstract method. No implementation.')

    @abstractmethod
    def run(self):
        raise NotImplementedError('Abstract method. No implementation.')

    @abstractmethod
    def save(self):
        raise NotImplementedError('Abstract method. No implementation.')

    @staticmethod
    def compare_fields(pairs, left, right, compare_fn, **args):
        logger.debug('>>--- compare_fields --->>')

        s1 = pairs[left]
        s2 = pairs[right]

        logger.info("Compare Function : %s", compare_fn)
        logger.debug('<<--- compare_fields ---<<')
        return apply_comparison(s1, s2, compare_fn, **args)

    @staticmethod
    def match_records(pairs, left_fields, right_fields, comparisons_methods):
        logger.debug('>>--- match_records --->>')
        logger.info('Applying linking rules.')

        pairs['matched'] = 1
        for left, right, fn in zip(left_fields, right_fields,
                                   comparisons_methods):
            method = fn.get('name', 'EXACT')
            args = fn.get('args') or {}
            logger.info("Left : %s, Right: %s, Args: %s", left, right, fn)
            result = LinkBase.compare_fields(pairs, left, right, method, **args)
            pairs['matched'] &= result

        pairs = pairs.loc[lambda df: df.matched == 1, :]
        pairs = pairs.drop('matched', axis=1)
        pairs = pairs.sort_index()

        logger.debug('<<--- match_records ---<<')
        return pairs

    def __init__(self, project):

        self.project = project
        self.project_type = project['type']
        self.left_file = None
        self.right_file = None
        self.left_index = None
        self.right_index = None
        self.left_fields = None
        self.right_fields = None
        self.left_dtypes = None
        self.right_dtypes = None
        self.left_columns = self.right_columns = []
        self.output_root = self.project['output_root']
        self.temp_path = self.project['temp_path']
        self.steps = None
        self.linked = None
        self.total_records_linked = 0
        self.total_entities = 0
        self.total_linked = None
        self.transformations = None
        self.comparison_methods = None

        for step in project['steps']:
            self.left_columns = list(set(self.left_columns +
                                         step['blocking_schema'].get('left', []) +
                                         step['linking_schema'].get('left', [])))
            self.right_columns = list(set(self.right_columns +
                                          step['blocking_schema'].get('right', []) +
                                          step['linking_schema'].get('right', [])))

    def __str__(self):
        if self.project is None:
            return ''

        data_dict = {
            "Name": self.project['name'],
            "Description": self.project['description'],
            "Steps": []
        }

        for step in self.project['steps']:
            step_dict = {
                "Seq": step['seq'],
                # "Name": step['name'], # Not used
                "Blocking": step['blocking_schema'],
                "Linking": step['linking_schema']
            }
            data_dict['Steps'].append(step_dict)

        return json.dumps(data_dict, indent=4)

    def pair_records(self, left_chunk, right_chunk, left_fields, right_fields, transformations):
        logger.debug('>>--- pair_records --->>')
        logger.info('Applying blocking rules.')

        left_index = 'LEFT_' + self.left_index
        right_index = 'RIGHT_' + self.right_index

        # Remove all rows that their blocking columns are empty.
        left_chunk = left_chunk.sort_index()
        left_chunk.replace(r'^\s+$', np.nan, regex=True, inplace=True)
        left_chunk = left_chunk.dropna(axis=0, how='any', subset=np.unique(left_fields))

        right_chunk = right_chunk.sort_index()
        right_chunk.replace(r'^\s+$', np.nan, regex=True, inplace=True)
        right_chunk = right_chunk.dropna(axis=0, how='any', subset=np.unique(right_fields))

        # Create copies of blocking columns to apply encoding methods
        left_on = [field + '_T' for field in left_fields]
        # left_chunk[left_on] = left_chunk[left_fields]
        left_chunk = left_chunk.assign(**{
            col + '_T': left_chunk[col] for col in left_fields
        })

        # Apply blocking variable encodings.
        for left, method in zip(left_on, transformations):
            left_chunk.loc[:, left] = apply_encoding(left_chunk[left], method)

        # Create copies of blocking columns to apply encoding methods
        right_on = [field + '_T' for field in right_fields]
        # right_chunk[right_on] = right_chunk[right_fields]
        right_chunk = right_chunk.assign(**{
            col + '_T': right_chunk[col] for col in right_fields
        })

        # Apply blocking variable encodings.
        for right, method in zip(right_on, transformations):
            right_chunk.loc[:, right] = apply_encoding(right_chunk[right], method)

        # The following too line are required to reset the index names in case if the data frames were emptied
        # after removing rows that have no values for the blocking variables. Otherwise the merge command will fail.
        left_chunk.index.names = [left_index]
        right_chunk.index.names = [right_index]

        chunk_pairs = left_chunk.reset_index().merge(
            right_chunk.reset_index(),
            how='inner',
            left_on=left_on,
            right_on=right_on,
        )

        # Skip comparing a record with itself for de-duplication projects
        if self.project_type == 'DEDUP':
            chunk_pairs = chunk_pairs.loc[chunk_pairs[left_index] < chunk_pairs[right_index]]

        chunk_pairs = chunk_pairs.set_index([left_index, right_index])

        # Remove temporary columns.
        chunk_pairs = chunk_pairs.drop(left_on + right_on, axis=1)

        logger.debug('<<--- pair_records ---<<')
        return chunk_pairs

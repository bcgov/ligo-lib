from __future__ import print_function

import json

import numpy as np
import pandas as pd

from .algorithms import apply_encoding, apply_comparison
from .base import (CHUNK_SIZE, _save_pairs)

import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class LinkBase(object):
    LEFT_INDEX = 'LEFT_ID'
    RIGHT_INDEX = 'RIGHT_ID'

    LEFT_ENTITY_ID = 'LEFT_EID'
    RIGHT_ENTITY_ID = 'RIGHT_EID'

    BLOCK_SIZE = 1000000

    id = 0

    @classmethod
    def getNextId(cls):
        cls.id += 1
        return cls.id

    @classmethod
    def resetId(cls):
        cls.id = 0

    def __init__(self, project):
        self.project = project
        self.project_type = project['type']
        self.left_dataset = None
        self.right_dataset = None
        self.left_index = None
        self.right_index = None
        self.left_fields = None
        self.right_fields = None
        self.left_columns = self.right_columns = []
        self.output_root = self.project['output_root'] or './'
        self.steps = None
        self.linked = None
        self.total_records_linked = 0
        self.total_entities = 0
        self.total_linked = None

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
                "Name": step['name'],
                "Blocking": step['blocking_schema'],
                "Linking": step['linking_schema']
            }
            data_dict['Steps'].append(step_dict)

        return json.dumps(data_dict, indent=4)

    @staticmethod
    def get_rows_in(data, match_index, level=None):
        if level is None:
            index = match_index
        else:
            index = match_index.get_level_values(level)
        rows = data.loc[index]
        rows.index = match_index
        return rows

    @staticmethod
    def get_rows_not_in(data, match_index, level=None):
        if level is None:
            index = data.index.difference(match_index)
        else:
            index = data.index.difference(match_index.get_level_values(level))
        rows = data.loc[index]
        return rows

    @staticmethod
    def _compare(pairs, left, right, compare_fn, **args):

        s1 = pairs[left]
        s2 = pairs[right]
        print("Compare Function : {0}".format(compare_fn))

        return apply_comparison(s1, s2, compare_fn, **args)

    def __pair_records(self, left_chunk, right_chunk, left_fields, right_fields, transformations):

        left_fields = ['LEFT_' + field for field in left_fields]
        right_fields = ['RIGHT_' + field for field in right_fields]

        left_chunk = left_chunk.sort_index()
        left_chunk.replace(r'^\s+$', np.nan, regex=True, inplace=True)
        left_chunk = left_chunk.dropna(axis=0, how='any', subset=np.unique(left_fields))

        right_chunk = right_chunk.sort_index()
        right_chunk.replace(r'^\s+$', np.nan, regex=True, inplace=True)
        right_chunk = right_chunk.dropna(axis=0, how='any', subset=np.unique(right_fields))

        # Create a copy of blocking columns to apply encoding methods
        left_on = [field + '_T' for field in left_fields]
        left_chunk[left_on] = left_chunk[left_fields]

        # Create a copy of blocking columns to apply encoding methods
        right_on = [field + '_T' for field in right_fields]
        right_chunk[right_on] = right_chunk[right_fields]

        # Apply blocking variable encodings.
        for left, method in zip(left_on, transformations):
            left_chunk.loc[:, left] = apply_encoding(left_chunk[left], method)

        # Apply blocking variable encodings.
        for right, method in zip(right_on, transformations):
            right_chunk.loc[:, right] = apply_encoding(right_chunk[right], method)

        chunk_pairs = left_chunk.reset_index().merge(
            right_chunk.reset_index(),
            how='inner',
            left_on=left_on,
            right_on=right_on
        )

        left_index = 'LEFT_' + self.left_index
        right_index = 'RIGHT_' + self.right_index
        if self.project_type == 'DEDUP':
            chunk_pairs = chunk_pairs.loc[chunk_pairs[left_index] < chunk_pairs[right_index]]

        chunk_pairs = chunk_pairs.set_index([left_index, right_index])

        chunk_pairs.drop(left_on + right_on, axis=1, inplace=True)

        return chunk_pairs

    def __match_records(self, pairs, left_fields, right_fields, comparisons_methods):
        pairs['matched'] = 1
        for left, right, fn in zip(left_fields, right_fields, comparisons_methods):
            method = fn.get('name', 'EXACT')
            args = fn.get('args') or {}
            print("Left : {0}, Right: {1}, Args: {2} ".format(left, right, fn))
            result = self._compare(pairs, left, right, method, **args)
            pairs['matched'] &= result

        pairs = pairs.loc[lambda df: df.matched == 1, :]

        pairs.drop('matched', axis=1, inplace=True)

        pairs = pairs.sort_index()
        return pairs

    def pair_n_match(self, step, link_method, blocking, linking):
        """
             TODO : Throw error if different number of left and right blocking variables are given.
             For each blocking variable there must be a corresponding  encoding method if encoding_method
             id not None.
         """

        append = False
        match_file_path = self.output_root + "matched_temp.csv"

        left_df = self.left_dataset

        left_fields = blocking.get('left')
        if self.project_type == 'DEDUP' and (blocking.get('right') is None or len(blocking.get('right')) == 0):
            right_fields = left_fields
        else:
            right_fields = blocking.get('right')

        print (right_fields)

        transformations = blocking.get('transformations')

        left_link_fields = linking.get('left')
        if self.project_type == 'DEDUP' and (linking.get('right') is None or len(linking.get('right')) == 0):
            right_link_fields = left_link_fields
        else:
            right_link_fields = linking.get('right')

        left_link_fields = ['LEFT_' + field for field in left_link_fields]
        right_link_fields = ['RIGHT_' + field for field in right_link_fields]
        comparison_methods = linking.get('comparisons')

        if self.project_type == 'DEDUP':
            right_df = left_df
        else:
            right_df = self.right_dataset

        left_chunks = int(np.ceil(len(left_df.index) / float(CHUNK_SIZE)))
        right_chunks = int(np.ceil(len(right_df.index) / float(CHUNK_SIZE)))

        for i in range(0, left_chunks):
            left_block = left_df.iloc[i * CHUNK_SIZE: (i + 1) * CHUNK_SIZE]
            left_block.columns = ['LEFT_' + col for col in left_block.columns]
            left_block.index.names = ['LEFT_' + left_block.index.name]

            for j in range(0, right_chunks):
                if self.project_type == 'DEDUP' and i > j: continue

                right_block = right_df.iloc[j * CHUNK_SIZE: (j + 1) * CHUNK_SIZE]

                right_block.columns = ['RIGHT_' + col for col in right_block.columns]
                right_block.index.names = ['RIGHT_' + right_block.index.name]

                print("Finding record pairs for left block {0} and right block {1}".format(i, j))
                pairs = self.__pair_records(left_block,
                                            right_block,
                                            left_fields, right_fields, transformations)

                if len(pairs.index) == 0:
                    continue

                matched = self.__match_records(pairs,
                                               left_link_fields,
                                               right_link_fields,
                                               comparison_methods)
                if self.project_type == 'LINK':
                    matched = matched[['LEFT_' + self.left_entity, 'RIGHT_' + self.right_entity]]
                else:
                    matched = pd.DataFrame(index=matched.index)

                matched['STEP'] = step
                matched = matched.sort_index()

                _save_pairs(match_file_path, matched, append)
                append = True

    def link(self):
        NotImplemented

    def load_data(self):
        for step in self.project['steps']:
            self.left_columns = list(set(self.left_columns +
                                         step['blocking_schema'].get('left', []) +
                                         step['linking_schema'].get('left', [])))
            self.right_columns = list(set(self.right_columns +
                                          step['blocking_schema'].get('right', []) +
                                          step['linking_schema'].get('right', [])))

    def run(self):
        NotImplemented

    def save(self):
        NotImplemented

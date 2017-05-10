from __future__ import print_function

import os
import csv
import json
import pandas as pd
import numpy as np

from cdilinker.linker.algorithms import apply_encoding, apply_comparison
from cdilinker.linker.base import (CHUNK_SIZE)

import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class LinkBase(object):
    LEFT_INDEX = 'LEFT_ID'
    RIGHT_INDEX = 'RIGHT_ID'

    LEFT_ENTITY_ID = 'LEFT_EID'
    RIGHT_ENTITY_ID = 'RIGHT_EID'

    id = 0

    @classmethod
    def getNextId(cls):
        cls.id += 1
        return cls.id

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

    def select_rows_in(self, data_file, index_col, index):

        reader = pd.read_csv(data_file, index_col=index_col, skipinitialspace=True, chunksize=CHUNK_SIZE)

        selected = pd.DataFrame()

        for chunk in reader:
            selected = selected.append(chunk.loc[index])

        return selected

    def __init__(self, project):

        self.project = project
        self.project_type = project['type']
        self.left_file = None
        self.right_file = None
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
                "Name": step['name'],
                "Blocking": step['blocking_schema'],
                "Linking": step['linking_schema']
            }
            data_dict['Steps'].append(step_dict)

        return json.dumps(data_dict, indent=4)

    @staticmethod
    def _compare(pairs, left, right, compare_fn, **args):

        s1 = pairs[left]
        s2 = pairs[right]
        print("Compare Function : {0}".format(compare_fn))

        return apply_comparison(s1, s2, compare_fn, **args)

    @staticmethod
    def append_rows(append_filename, source_filename, first_batch=True):

        if first_batch:
            os.rename(source_filename, append_filename)
        else:
            with open(append_filename, 'a') as append_file, \
                    open(source_filename, 'r') as matched_file:
                for row_no, row in enumerate(matched_file):
                    if row_no > 0:
                        append_file.write(row)

    def __pair_records(self, left_chunk, right_chunk, left_fields, right_fields, transformations):

        left_chunk = left_chunk.dropna(axis=0, how='any', subset=np.unique(left_fields))

        # Create a copy of blocking columns to apply encoding methods
        left_on = [field + '_T' for field in left_fields]
        left_chunk[left_on] = left_chunk[left_fields]

        # Apply blocking variable encodings.
        for left, method in zip(left_on, transformations):
            left_chunk.loc[:, left] = apply_encoding(left_chunk[left], method)

        right_chunk = right_chunk.dropna(axis=0, how='any', subset=np.unique(right_fields))

        # Create a copy of blocking columns to apply encoding methods
        right_on = [field + '_T' for field in right_fields]
        right_chunk[right_on] = right_chunk[right_fields]

        # Apply blocking variable encodings.
        for right, method in zip(right_on, transformations):
            right_chunk.loc[:, right] = apply_encoding(right_chunk[right], method)

        chunk_pairs = left_chunk.reset_index().merge(
            right_chunk.reset_index(),
            how='inner',
            left_on=left_on,
            right_on=right_on,
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

    def pair_n_match(self, step, link_method, blocking, linking, matched_file):

        temp_file = self.output_root + "matched_temp.csv"

        total_pairs = 0
        shared = 0

        '''
        Prefix each column in left data with 'LEFT_' and the right data columns with 'RIGHT_'
        to avoid name conflicts on merging two data chunks.
        '''
        left_reader = pd.read_csv(self.left_file,
                                  index_col=[self.left_index],
                                  usecols=self.left_columns,
                                  skipinitialspace=True,
                                  chunksize=CHUNK_SIZE)

        for left_chunk_no, left_chunk in enumerate(left_reader):

            # Read right file chunk by chunck and merge each chunk with the current left chunk
            right_reader = pd.read_csv(self.right_file,
                                       index_col=[self.right_index],
                                       usecols=self.right_columns,
                                       skipinitialspace=True,
                                       chunksize=CHUNK_SIZE)

            left_chunk.columns = ['LEFT_' + col for col in left_chunk.columns]
            left_chunk.index.names = ['LEFT_' + left_chunk.index.name]

            for right_chunk_no, right_chunk in enumerate(right_reader):

                print("Finding record pairs for left block {0} and right block {1}".format(left_chunk_no,
                                                                                           right_chunk_no))
                if self.project_type == 'DEDUP' and left_chunk_no > right_chunk_no:
                    continue

                '''
                Prefix each column in left data with 'LEFT_' and the right data columns with 'RIGHT_'
                to avoid name conflicts on merging two data chunks.
                '''

                right_chunk.columns = ['RIGHT_' + col for col in right_chunk.columns]
                right_chunk.index.names = ['RIGHT_' + right_chunk.index.name]

                left_chunk = left_chunk.sort_index()
                right_chunk = right_chunk.sort_index()

                left_fields = blocking.get('left')
                if self.project_type == 'DEDUP':
                    right_fields = left_fields
                else:
                    right_fields = blocking.get('right')

                left_fields = ['LEFT_' + field for field in left_fields]
                right_fields = ['RIGHT_' + field for field in right_fields]

                transformations = blocking.get('transformations')

                pairs = self.__pair_records(left_chunk,
                                            right_chunk,
                                            left_fields, right_fields, transformations)

                if len(pairs.index) == 0:
                    continue
                left_fields = linking.get('left')
                if self.project_type == 'DEDUP':
                    right_fields = left_fields
                else:
                    right_fields = linking.get('right')

                left_fields = ['LEFT_' + field for field in left_fields]
                right_fields = ['RIGHT_' + field for field in right_fields]
                comparison_methods = linking.get('comparisons')
                matched = self.__match_records(pairs,
                                               left_fields,
                                               right_fields,
                                               comparison_methods)
                matched[self.project_type + '_STEP'] = step
                matched = matched.sort_index()

                left_index = 'LEFT_' + self.left_index
                right_index = 'RIGHT_' + self.right_index
                merge_columns = [left_index, right_index]

                with open(matched_file, 'r') as in_file, open(temp_file, 'w') as merge_file:
                    reader = csv.reader(in_file)
                    merge_writer = csv.writer(merge_file)
                    matched = matched.reset_index()
                    try:
                        header = next(reader)
                    except StopIteration as e:
                        header = matched.columns
                    matched_reader = iter(matched.values.tolist())
                    total_pairs = self.merge(left_reader=reader,
                                             right_reader=matched_reader,
                                             header=header,
                                             columns=merge_columns,
                                             csv_writer=merge_writer)

                os.remove(matched_file)
                os.rename(temp_file, matched_file)

        return total_pairs

    def merge(self, left_reader, right_reader, header, columns, csv_writer):

        csv_writer.writerow(header)
        header_index = {key: index for index, key in enumerate(header)}
        col_index = [header_index[col] for col in columns]

        left_row = right_row = None

        count = 0
        try:
            left_row = next(left_reader)
            right_row = next(right_reader)

            while True:
                left_cols = [left_row[i] for i in col_index]
                right_cols = [right_row[i] for i in col_index]
                left_cols = [float(x) for x in left_cols]
                right_cols = [float(x) for x in right_cols]
                count += 1
                if left_cols < right_cols:
                    csv_writer.writerow(left_row)
                    left_row = None
                    left_row = next(left_reader)
                elif left_cols > right_cols:
                    csv_writer.writerow(right_row)
                    right_row = None
                    right_row = next(right_reader)
                else:
                    csv_writer.writerow(left_row)
                    left_row = right_row = None
                    left_row = next(left_reader)
                    right_row = next(right_reader)
        except StopIteration as e:
            pass

        if left_row is not None:
            csv_writer.writerow(left_row)
            count += 1
        if right_row is not None:
            csv_writer.writerow(right_row)
            count += 1

        for left_row in left_reader:
            csv_writer.writerow(left_row)
            count += 1
        for right_row in right_reader:
            csv_writer.writerow(right_row)
            count += 1

        return count

    def import_data(self, src_filename, columns, dest_filename, rename_cols=None):
        '''
        Reads and imports the selected columns of a csv file into a new csv file.
        The copied files is used during linking process to leave the source file unchanged.
        :param src_file: Original csv file
        :param columns: Columns from the file that need to be imported.
        :param dest_file: Copied file with selected column
        :return:
        '''

        open(dest_filename, 'w').close()
        reader = pd.read_csv(src_filename, usecols=columns, skipinitialspace=True, chunksize=CHUNK_SIZE)
        with open(dest_filename, 'a') as dest_file:
            first_chunk = True
            for chunk in reader:
                if first_chunk and rename_cols is not None:
                    chunk.rename(columns=rename_cols, inplace=True)
                chunk.replace(np.nan, '', regex=True)
                chunk.to_csv(dest_file, index=False, header=first_chunk)
                first_chunk = False

    def load_data(self):
        NotImplemented

    def run(self):
        NotImplemented

    def save(self):
        NotImplemented

import os
import csv
import json
import pandas as pd
import numpy as np

from cdilinker.linker.algorithms import apply_encoding, apply_comparison
from cdilinker.linker.base import (CHUNK_SIZE)

from cdilinker.linker.files import LinkFiles

import logging

logger = logging.getLogger(__name__)


class LinkBase(object):
    # Suppress SettingWithCopyWarning warnings from Pandas
    # https://stackoverflow.com/q/20625582
    pd.options.mode.chained_assignment = None  # default='warn'

    LEFT_INDEX = 'LEFT_ID'
    RIGHT_INDEX = 'RIGHT_ID'

    LEFT_ENTITY_ID = 'LEFT_EID'
    RIGHT_ENTITY_ID = 'RIGHT_EID'

    id = 0

    @classmethod
    def get_next_id(cls):
        cls.id += 1
        return cls.id

    @classmethod
    def reset_id(cls):
        cls.id = 0

    @staticmethod
    def get_rows_in(data, match_index, level=None):
        logger.debug('>>--- get_rows_in --->>')
        if level is None:
            index = match_index
        else:
            index = match_index.get_level_values(level)
        rows = data.loc[index]
        rows.index = match_index
        logger.debug('<<--- get_rows_in ---<<')
        return rows

    @staticmethod
    def get_rows_not_in(data, match_index, level=None):
        logger.debug('>>--- get_rows_not_in --->>')
        if level is None:
            index = data.index.difference(match_index)
        else:
            index = data.index.difference(match_index.get_level_values(level))
        rows = data.loc[index]
        logger.debug('<<--- get_rows_not_in ---<<')
        return rows

    def select_rows_in(self, data_file, index_col, index):
        logger.debug('>>--- select_rows_in --->>')

        reader = pd.read_csv(data_file, index_col=index_col, skipinitialspace=True, chunksize=CHUNK_SIZE)

        selected = pd.DataFrame()

        for chunk in reader:
            selected = selected.append(chunk.loc[index])

        logger.debug('<<--- select_rows_in ---<<')
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

    @staticmethod
    def compare_fields(pairs, left, right, compare_fn, **args):
        logger.debug('>>--- compare_fields --->>')

        s1 = pairs[left]
        s2 = pairs[right]

        logger.info("Compare Function : %s", compare_fn)
        logger.debug('<<--- compare_fields ---<<')
        return apply_comparison(s1, s2, compare_fn, **args)

    @staticmethod
    def append_rows(append_filename, source_filename, first_batch=True):
        logger.debug('>>--- append_rows --->>')

        if first_batch:
            os.rename(source_filename, append_filename)
        else:
            with open(append_filename, 'a') as append_file, \
                    open(source_filename, 'r') as matched_file:
                for row_no, row in enumerate(matched_file):
                    if row_no > 0:
                        append_file.write(row)

        logger.debug('<<--- append_rows ---<<')

    def pair_records(self, left_chunk, right_chunk, left_fields, right_fields, transformations):

        logger.debug('>>--- pair_records --->>')

        logger.info('Applying blocking rules.')

        left_index = 'LEFT_' + self.left_index
        right_index = 'RIGHT_' + self.right_index

        # Remove all rows that their blocking columns are empty.
        left_chunk.replace(r'^\s+$', np.nan, regex=True, inplace=True)
        left_chunk = left_chunk.dropna(axis=0, how='any', subset=np.unique(left_fields))

        right_chunk.replace(r'^\s+$', np.nan, regex=True, inplace=True)
        right_chunk = right_chunk.dropna(axis=0, how='any', subset=np.unique(right_fields))

        # Create a copy of blocking columns to apply encoding methods
        left_on = [field + '_T' for field in left_fields]
        left_chunk[left_on] = left_chunk[left_fields]

        # Apply blocking variable encodings.
        for left, method in zip(left_on, transformations):
            left_chunk.loc[:, left] = apply_encoding(left_chunk[left], method)

        # Create a copy of blocking columns to apply encoding methods
        right_on = [field + '_T' for field in right_fields]
        right_chunk[right_on] = right_chunk[right_fields]

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
        chunk_pairs.drop(left_on + right_on, axis=1, inplace=True)

        logger.debug('<<--- pair_records ---<<')
        return chunk_pairs

    def match_records(self, pairs, left_fields, right_fields, comparisons_methods):

        logger.debug('>>--- match_records --->>')
        logger.info('Applying linking rules.')

        pairs['matched'] = 1
        for left, right, fn in zip(left_fields, right_fields, comparisons_methods):
            method = fn.get('name', 'EXACT')
            args = fn.get('args') or {}
            logger.info("Left : %s, Right: %s, Args: %s", left, right, fn)
            result = self.compare_fields(pairs, left, right, method, **args)

            pairs['matched'] &= result

        pairs = pairs.loc[lambda df: df.matched == 1, :]

        pairs.drop('matched', axis=1, inplace=True)

        pairs = pairs.sort_index()

        logger.debug('<<--- match_records ---<<')
        return pairs

    def pair_n_match(self, step, link_method, blocking, linking, matched_file):
        logger.debug('>>--- pair_n_match --->>')
        logger.info('Finding matched records.')
        logger.debug('Blocking variables: %s', blocking)
        logger.debug('Linking variables: %s', linking)

        temp_file = self.temp_path + LinkFiles.TEMP_MATCHED_FILE

        total_pairs = 0

        # Prefix each column in left data with 'LEFT_' and the right data
        # columns with 'RIGHT_' to avoid name conflicts on merging two data
        # chunks.

        logger.info('Readding input data file chunk by chunk')
        left_reader = pd.read_csv(self.left_file,
                                  index_col=[self.left_index],
                                  usecols=self.left_columns,
                                  skipinitialspace=True,
                                  dtype=self.left_dtypes,
                                  chunksize=CHUNK_SIZE)

        for left_chunk_no, left_chunk in enumerate(left_reader):

            # Read right file chunk by chunck and merge each chunk with the current left chunk
            right_reader = pd.read_csv(self.right_file,
                                       index_col=[self.right_index],
                                       usecols=self.right_columns,
                                       skipinitialspace=True,
                                       dtype=self.right_dtypes,
                                       chunksize=CHUNK_SIZE)

            left_chunk.columns = ['LEFT_' + col for col in left_chunk.columns]
            left_chunk.index.names = ['LEFT_' + left_chunk.index.name]

            for right_chunk_no, right_chunk in enumerate(right_reader):

                logger.info("Finding record pairs for left block %s and right block %s",
                            left_chunk_no, right_chunk_no)
                if self.project_type == 'DEDUP' and left_chunk_no > right_chunk_no:
                    continue

                # Prefix each column in left data with 'LEFT_' and the right
                # data columns with 'RIGHT_' to avoid name conflicts on merging
                # two data chunks.

                right_chunk.columns = ['RIGHT_' + col for col in right_chunk.columns]
                right_chunk.index.names = ['RIGHT_' + right_chunk.index.name]

                left_chunk = left_chunk.sort_index()
                right_chunk = right_chunk.sort_index()

                left_fields = blocking.get('left')
                if self.project_type == 'DEDUP' and (blocking.get('right') is None or len(blocking.get('right')) == 0):
                    right_fields = left_fields
                else:
                    right_fields = blocking.get('right')

                left_fields = ['LEFT_' + field for field in left_fields]
                right_fields = ['RIGHT_' + field for field in right_fields]

                transformations = blocking.get('transformations')

                pairs = self.pair_records(left_chunk,
                                          right_chunk,
                                          left_fields, right_fields, transformations)

                if len(pairs.index) == 0:
                    continue
                left_fields = linking.get('left')
                if self.project_type == 'DEDUP' and (linking.get('right') is None or len(linking.get('right')) == 0):
                    right_fields = left_fields
                else:
                    right_fields = linking.get('right')

                left_fields = ['LEFT_' + field for field in left_fields]
                right_fields = ['RIGHT_' + field for field in right_fields]
                comparison_methods = linking.get('comparisons')
                matched = self.match_records(pairs,
                                             left_fields,
                                             right_fields,
                                             comparison_methods)
                matched[self.project_type + '_STEP'] = step
                matched = matched.sort_index()

                left_index = 'LEFT_' + self.left_index
                right_index = 'RIGHT_' + self.right_index
                merge_columns = [left_index, right_index]

                # Move index columns to the front
                if self.project_type == 'LINK':
                    entity_cols = ['LEFT_' + self.left_entity, 'RIGHT_' + self.right_entity]
                    cols = matched.columns.tolist()
                    cols = entity_cols + [x for x in cols if x not in entity_cols]
                    matched = matched[cols]

                # Replace all empty cells with empty string to avoid writing nan in the csv file.
                matched.replace(np.nan, '', regex=True)

                logger.info('Merging chunk result into the matched records file.')
                with open(matched_file, 'r') as in_file, open(temp_file, 'w') as merge_file:
                    reader = csv.reader(in_file)
                    merge_writer = csv.writer(merge_file)
                    matched = matched.reset_index()
                    try:
                        header = next(reader)
                    except StopIteration:
                        header = matched.columns
                    matched_reader = iter(matched.values.tolist())
                    total_pairs = self.merge(left_reader=reader,
                                             right_reader=matched_reader,
                                             header=header,
                                             columns=merge_columns,
                                             csv_writer=merge_writer)

                if os.path.isfile(matched_file):
                    os.remove(matched_file)
                if os.path.isfile(temp_file):
                    os.rename(temp_file, matched_file)

        logger.info('Finding matched records is complete.')
        logger.debug('<<--- pair_n_match ---<<')
        return total_pairs

    @staticmethod
    def merge(left_reader, right_reader, header, columns, csv_writer):
        logger.debug('>>--- merge --->>')

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
        except StopIteration:
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

        logger.debug('Number of records merged: %s', count)
        logger.debug('<<--- merge ---<<')
        return count

    def import_data(self, src_filename, columns, dest_filename, front_cols=None, data_types=None):
        """
        Reads and imports the selected columns of a csv file into a new csv file.
        The copied files is used during linking process to leave the source file unchanged.
        :param src_file: Original csv file
        :param columns: Columns from the file that need to be imported.
        :param dest_file: Copied file with selected column
        :return:
        """

        logger.debug('>>--- import_data --->>')
        logger.info('Importing datafile %s...', src_filename)

        open(dest_filename, 'w').close()
        reader = pd.read_csv(src_filename, usecols=columns, skipinitialspace=True, chunksize=CHUNK_SIZE,
                             dtype=data_types)
        with open(dest_filename, 'a') as dest_file:
            first_chunk = True
            for chunk in reader:
                if front_cols is not None:
                    cols = chunk.columns.tolist()
                    cols = front_cols + [x for x in cols if x not in front_cols]
                    chunk = chunk[cols]
                chunk.replace(np.nan, '', regex=True)
                chunk.to_csv(dest_file, index=False, header=first_chunk)
                first_chunk = False

        logger.info('Datafile %s is imported successfully.', src_filename)
        logger.debug('<<--- import_data ---<<')

    def load_data(self):
        NotImplemented

    def run(self):
        NotImplemented

    def save(self):
        NotImplemented

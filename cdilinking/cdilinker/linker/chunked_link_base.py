import os
import csv
import pandas as pd
import numpy as np

from cdilinker.linker.base import (CHUNK_SIZE)

from cdilinker.linker.link_base import LinkBase
from cdilinker.linker.files import LinkFiles

import logging

logger = logging.getLogger(__name__)


class ChunkedLinkBase(LinkBase):
    def __init__(self, project):
        super(ChunkedLinkBase, self).__init__(project)

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

        # Get left and right blocking variables and rules. We need to add different left and right prefixes
        # to avoid name conflicts.
        left_block_fields = blocking.get('left')
        if self.project_type == 'DEDUP' and (blocking.get('right') is None or len(blocking.get('right')) == 0):
            right_block_fields = left_block_fields
        else:
            right_block_fields = blocking.get('right')

        left_block_fields = ['LEFT_' + field for field in left_block_fields]
        right_block_fields = ['RIGHT_' + field for field in right_block_fields]

        transformations = blocking.get('transformations')

        # Get left and right linking variables and comparison algorithms.
        left_fields = linking.get('left')
        if self.project_type == 'DEDUP' and (linking.get('right') is None or len(linking.get('right')) == 0):
            right_fields = left_fields
        else:
            right_fields = linking.get('right')

        left_fields = ['LEFT_' + field for field in left_fields]
        right_fields = ['RIGHT_' + field for field in right_fields]
        comparison_methods = linking.get('comparisons')

        left_index = 'LEFT_' + self.left_index
        right_index = 'RIGHT_' + self.right_index
        merge_columns = [left_index, right_index]

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

                pairs = self.pair_records(left_chunk,
                                          right_chunk,
                                          left_block_fields, right_block_fields, transformations)

                if len(pairs.index) == 0:
                    continue

                matched = LinkBase.match_records(pairs,
                                                 left_fields,
                                                 right_fields,
                                                 comparison_methods)

                matched[self.project_type + '_STEP'] = step
                matched = matched.sort_index()

                # Move index columns to the front
                if self.project_type == 'LINK':
                    entity_cols = ['LEFT_' + self.left_entity, 'RIGHT_' + self.right_entity]
                    cols = matched.columns.tolist()
                    cols = entity_cols + [x for x in cols if x not in entity_cols]
                    matched = matched[cols]

                # Replace all empty cells with empty string to avoid writing nan in the csv file.
                matched.replace(np.nan, '', regex=True)

                logger.info('Merging chunk result into the matched records file.')
                with open(matched_file, 'r') as in_file, \
                        open(temp_file, 'w') as merge_file:
                    reader = csv.reader(in_file)
                    merge_writer = csv.writer(merge_file, lineterminator='\n')
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

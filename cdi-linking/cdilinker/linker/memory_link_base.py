import json

import numpy as np
import pandas as pd

from abc import abstractmethod

from cdilinker.linker.base import (CHUNK_SIZE, _save_pairs)
from cdilinker.linker.files import LinkFiles
from cdilinker.linker.link_base import LinkBase

import logging

logger = logging.getLogger(__name__)


class MemoryLinkBase(LinkBase):

    def __init__(self, project):
        super(MemoryLinkBase, self).__init__(project)

    @staticmethod
    def get_rows_in(data, match_index):

        rows = data.loc[match_index]
        rows.index = match_index
        return rows

    @staticmethod
    def get_rows_not_in(data, match_index):

        index = data.index.difference(match_index)
        rows = data.loc[index]
        return rows

    def pair_n_match(self, step, link_method, blocking, linking):
        """
             TODO : Throw error if different number of left and right blocking
             variables are given. For each blocking variable there must be a
             corresponding encoding method if encoding_method id not None.
        """

        logger.debug('>>--- pair_n_match --->>')
        logger.info('Finding records pairs that satisfy blocking and linking rules.')
        append = False
        match_file_path = self.temp_path + LinkFiles.TEMP_MATCHED_FILE

        left_df = self.left_dataset

        left_fields = blocking.get('left')
        if self.project_type == 'DEDUP' and (not blocking.get('right')):
            right_fields = left_fields
        else:
            right_fields = blocking.get('right')

        left_fields = ['LEFT_' + field for field in left_fields]
        right_fields = ['RIGHT_' + field for field in right_fields]

        transformations = blocking.get('transformations')

        left_link_fields = linking.get('left')
        if self.project_type == 'DEDUP' and (not linking.get('right')):
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
                if self.project_type == 'DEDUP' and i > j:
                    continue

                right_block = right_df.iloc[j * CHUNK_SIZE: (j + 1) * CHUNK_SIZE]

                right_block.columns = ['RIGHT_' + col for col in
                                       right_block.columns]
                right_block.index.names = ['RIGHT_' + right_block.index.name]

                logger.info("Finding record pairs for left block %s and right block %s",
                            i, j)
                pairs = self.pair_records(left_block,
                                          right_block,
                                          left_fields, right_fields, transformations)

                if len(pairs.index) == 0:
                    continue

                matched = LinkBase.match_records(pairs,
                                                 left_link_fields,
                                                 right_link_fields,
                                                 comparison_methods)
                if self.project_type == 'LINK':
                    matched = matched[['LEFT_' + self.left_entity,
                                       'RIGHT_' + self.right_entity]]
                else:
                    matched = pd.DataFrame(index=matched.index)

                matched['STEP'] = step
                matched = matched.sort_index()

                _save_pairs(match_file_path, matched, append)
                append = True

        logger.debug('<<--- pair_n_match ---<<')

    @abstractmethod
    def link(self):
        raise NotImplementedError("Abstract method")


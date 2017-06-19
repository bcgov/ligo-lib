import os
import json
import numpy as np
import pandas as pd

import logging

from .base import (link_config,
                   CHUNK_SIZE,
                   COLUMN_TYPES,
                   _save_pairs,
                   sort_csv)
from cdilinker.reports.report import generate_linking_summary

from .chunked_link_base import LinkBase

from cdilinker.linker.files import LinkFiles


logger = logging.getLogger(__name__)


class DeDeupProject(LinkBase):
    def __init__(self, project):

        super(DeDeupProject, self).__init__(project)

        self.project_type = 'DEDUP'
        dataset = project['datasets'][0]
        self.left_index = self.right_index = dataset['index_field']
        self.matched = None
        self.left_dtypes = self.right_dtypes = None

    def __str__(self):

        descriptor = super(DeDeupProject, self).__str__()

        data_dict = json.loads(descriptor)
        dataset = self.project['datasets'][0]
        data_dict['dataset'] = dataset['name']

        return json.dumps(data_dict, indent=4)

    def load_data(self):

        logger.debug('>>--- load_data --->>')
        logger.info('Loading input dataset for project: {0} with task id: {1}.'
                    .format(self.project['name'], self.project['task_uuid']))

        dataset = self.project['datasets'][0]
        self.left_columns.append(dataset['index_field'])

        if 'data_types' in dataset:
            left_dtypes = {}
            for col_name, col_type in dataset["data_types"].items():
                left_dtypes[col_name] = COLUMN_TYPES[col_type]
        else:
            left_dtypes = None

        try:
            usecols = dataset['columns'] or self.left_columns
        except KeyError:
            usecols = self.left_columns

        self.left_dtypes = self.right_dtypes = left_dtypes
        self.left_columns = self.right_columns = usecols

        logger.debug('Data columns: {0}.'.format(self.left_columns))
        logger.debug('Data types: {0}'.format(self.left_dtypes))

        self.right_file = self.left_file = self.output_root + link_config.get('left_file', 'left_file.csv')

        super(DeDeupProject, self).import_data(dataset['url'],
                                               usecols,
                                               self.left_file,
                                               front_cols=[self.left_index],
                                               data_types=self.left_dtypes)

        logger.debug('<<--- load_data ---<<')

    def link_pairs(self):
        logger.debug('>>--- link_pairs --->>')
        logger.info('Assigning entity id to linked records.')

        from cdilinker.linker.union_find import UnionFind
        matched_file = self.output_root + LinkFiles.MATCHED_RECORDS

        link_index = pd.Index([], name='REC_ID')
        left_index = 'LEFT_' + self.left_index
        right_index = 'RIGHT_' + self.right_index

        if not os.path.exists(matched_file) or os.path.getsize(matched_file) == 0:
            return 0

        matched_reader = pd.read_csv(matched_file,
                                     index_col=[left_index, right_index], chunksize=CHUNK_SIZE)

        logger.debug('Loading linked records chunk by chunk.')
        for chunk in matched_reader:
            # Create a union of left record and right record indices
            left_set = chunk.index.get_level_values(0).drop_duplicates()
            right_set = chunk.index.get_level_values(1).drop_duplicates()
            union_set = left_set.union(right_set)
            union_set = union_set.rename('REC_ID')

            # Merge the result with the union index of all record id's
            link_index = link_index.union(union_set)

        # Create mapping from selected records index to their position in disjoin set.
        rec_len = len(link_index)
        index_loc = pd.Series(list(range(rec_len)), index=link_index)
        entity_ids = [None] * rec_len

        # Create disjoint set of entities
        entts = UnionFind(rec_len)

        linked = pd.DataFrame(columns=['ENTITY_ID'], index=link_index)
        linked['ENTITY_ID'].fillna(0, inplace=True)

        # Find all connected records and make entity groups
        matched_reader = pd.read_csv(matched_file,
                                     index_col=[left_index, right_index], chunksize=CHUNK_SIZE)

        logger.debug('Finding chains of connected records that belong to the same entity')
        for chunk in matched_reader:
            for left_id, right_id in chunk.index.values:
                entts.union(index_loc[left_id], index_loc[right_id])

        # Assign entity id's
        append = False
        entity_file = self.output_root + LinkFiles.TEMP_MATCHED_ENTITY_FILE
        matched_reader = pd.read_csv(matched_file,
                                     index_col=[left_index, right_index], chunksize=CHUNK_SIZE)

        for chunk in matched_reader:
            chunk.insert(0, 'ENTITY_ID', np.nan)
            for left_id, right_id in chunk.index.values:
                entt = entts.find(index_loc[left_id])
                entity_ids[entt] = entity_ids[entt] or LinkBase.get_next_id()
                chunk.set_value((left_id, right_id), 'ENTITY_ID', entity_ids[entt])
                linked.set_value(left_id, 'ENTITY_ID', entity_ids[entt])
                linked.set_value(right_id, 'ENTITY_ID', entity_ids[entt])

            _save_pairs(entity_file, chunk, append)
            append = True

        if os.path.isfile(matched_file):
            os.remove(matched_file)
        if os.path.isfile(entity_file):
            os.rename(entity_file, matched_file)

        linked_file = self.output_root + LinkFiles.TEMP_ENTITIES_FILE
        linked.replace(np.nan, '', regex=True)
        linked.to_csv(linked_file, index=True)

        logger.debug('<<--- link_pairs ---<<')
        return entts.count()

    def extract_rows(self, data_filename, data_id, index_filename, index_id, index_cols, selected_filename=None):
        logger.debug('>>--- extract_rows --->>')

        logger.info('Removing all linked records from the input data file and preparing data for the next step.')

        import csv

        if selected_filename is None:
            selected_filename = self.output_root + LinkFiles.TEMP_DEDUP_STEP_SELECTED
        remained_filename = self.output_root + LinkFiles.TEMP_STEP_REMAINED

        with open(data_filename, 'r') as data_file, open(index_filename, 'r') as index_file, \
                open(selected_filename, 'w') as selected_file, open(remained_filename, 'w') as remained_file:

            data_reader = csv.reader(data_file)
            index_reader = csv.reader(index_file)

            selected_writer = csv.writer(selected_file)
            remained_writer = csv.writer(remained_file)

            # Read header rows
            data_header = next(data_reader)
            index_header = next(index_reader)

            # Get the position of required columns in data and index rows
            data_header_map = {key: index for index, key in enumerate(data_header)}
            data_idx = data_header_map[data_id]

            index_header_map = {key: index for index, key in enumerate(index_header)}
            index_idx = index_header_map[index_id]
            col_index = [index_header_map[col] for col in index_cols]

            # Write header rows for selected and remained rows
            selected_header = index_cols + data_header
            selected_writer.writerow(selected_header)
            remained_writer.writerow(data_header)

            try:
                data_row = next(data_reader)
                index_row = next(index_reader)

                while True:
                    if float(data_row[data_idx]) < float(index_row[index_idx]):
                        # Data row doses not exist in index file, so add it to remained rows
                        remained_writer.writerow(data_row)
                        data_row = None
                        data_row = next(data_reader)
                    else:
                        # Data row found in index file, so add it to selected rows.
                        row = [index_row[i] for i in col_index] + data_row
                        selected_writer.writerow(row)
                        data_row = None
                        data_row = next(data_reader)
                        index_row = None
                        index_row = next(index_reader)
            except StopIteration:
                pass

            if data_row is not None:
                remained_writer.writerow(data_row)

            # Add all remained rows in data file to remained rows file
            for data_row in data_reader:
                remained_writer.writerow(data_row)

        # Replace the data file with remained file
        if os.path.isfile(data_filename):
            os.remove(data_filename)

        if os.path.isfile(remained_filename):
            os.rename(remained_filename, data_filename)

        logger.debug('<<--- extract_rows ---<<')

    def run(self):
        '''
        Runs a de-duplication project consisting of a sequence of steps.
        Each step is defined by a set of blocking and linking identifiers and rules.
        :return: A de-duplicated version of the original data file and the de-duplication summary report.
        '''
        logger.debug('>>--- run --->>')
        logger.info('Executing de-duplication project {0}. Task id: {1}.'
                    .format(self.project['name'], self.project['task_uuid']))

        LinkBase.reset_id()
        self.steps = {}
        self.linked = pd.DataFrame()

        matched_file = self.output_root + LinkFiles.MATCHED_RECORDS

        selected_filename = self.output_root + LinkFiles.TEMP_DEDUP_STEP_SELECTED
        final_selected_file = self.output_root + LinkFiles.TEMP_DEDUP_ALL_SELECTED

        dedup_results_file = self.output_root + link_config.get('dedup_matched_file', 'dedup_matched.csv')

        linked_file = self.output_root + LinkFiles.TEMP_ENTITIES_FILE

        open(matched_file, 'w').close()

        linked_stats = {}
        prev_total = 0
        self.total_entities = 0
        first_batch = True
        for step in self.project['steps']:
            self.steps[step['seq']] = {}
            logger.info("De-duplication Step {0} :".format(step['seq']))
            logger.info("{0}.1) Finding record pairs satisfying blocking and linking constraints...".format(step['seq']))

            pairs_count = self.pair_n_match(step=step['seq'],
                                            link_method=step['linking_method'],
                                            blocking=step['blocking_schema'],
                                            linking=step['linking_schema'], matched_file=matched_file)

            linked_stats[step['seq']] = pairs_count - prev_total
            prev_total = pairs_count

            if step['group']:
                self.total_entities += self.link_pairs()

                self.extract_rows(data_filename=self.left_file, data_id=self.left_index,
                                  index_filename=linked_file, index_id='REC_ID', index_cols=['ENTITY_ID'])

                sort_csv(selected_filename, appendfile=final_selected_file,
                         cols=['ENTITY_ID'], types={'ENTITY_ID': 'numeric'})

                self.total_records_linked += pairs_count

                LinkBase.append_rows(dedup_results_file, matched_file, first_batch=first_batch)
                first_batch = False
                open(matched_file, 'w').close()
                prev_total = 0

        for step in self.project['steps']:
            self.steps[step['seq']]['total_records_linked'] = linked_stats.get(step['seq'], 0)

        if os.path.isfile(matched_file):
            os.remove(matched_file)
        if os.path.isfile(linked_file):
            os.remove(linked_file)
        if os.path.isfile(selected_filename):
            os.remove(selected_filename)

        logger.info('Execution of de-duplication project {0} with Task id: {1} is completed.'
                    .format(self.project['name'], self.project['task_uuid']))
        logger.debug('<<--- run ---<<')

    def save(self):
        """
        Create the de-duplicated output file sorted by entity id's and
        Generates the de-duplicated file.
        Preconditions: All de-duplication steps must be completed.
        :return: De-duplication summary report.
        """
        logger.debug('>>--- save --->>')

        logger.info('Saving results of the de-duplication project {0}-{1}'
                    .format(self.project['name'], self.project['task_uuid']))
        # Adding the selected (de-duped) entities to the final result
        selected_rows = self.output_root + LinkFiles.TEMP_DEDUP_ALL_SELECTED
        data_reader = pd.read_csv(self.left_file,
                                  usecols=self.left_columns, dtype=self.left_dtypes,
                                  skipinitialspace=True, chunksize=CHUNK_SIZE)

        # Storing deduplication result. It contains the original records plus the entity id of each record.
        deduped_file_path = self.output_root + link_config.get('deduped_data_file', 'deduped_data.csv')

        file_mode = 'a'
        header = False
        if os.path.isfile(selected_rows):
            os.rename(selected_rows, deduped_file_path)
        else:
            file_mode = 'w'
            header = True

        # Assign unique entity id to all remaining records.
        logger.info('Assigning entity id to all remaining records.')
        total_remained = 0
        with open(deduped_file_path, file_mode) as out_file:
            for chunk in data_reader:
                chunk.insert(0, 'ENTITY_ID', np.nan)
                for rec_id in chunk.index.values:
                    chunk.set_value(rec_id, 'ENTITY_ID', LinkBase.get_next_id())
                    total_remained += 1
                chunk.replace(np.nan, '', regex=True)
                chunk.to_csv(out_file, index=False, header=header)
                header = False

        # Total number of entities after de-duplication
        self.total_entities += total_remained
        logger.info('Total number of entities after de-duplication: {0}'.format(self.total_entities))

        logger.info('De-duplicated file generated at {0}.'.format(deduped_file_path))

        logger.debug('<<--- save ---<<')
        # Generating de-duplication summary report
        return generate_linking_summary(self, self.output_root)

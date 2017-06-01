from __future__ import print_function

import os
import json
import numpy as np
import pandas as pd

from .base import (CHUNK_SIZE,
                   COLUMN_TYPES,
                   _save_pairs,
                   sort_csv)
from cdilinker.reports.report import generate_linking_summary

from .chunked_link_base import LinkBase

import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class DeDeupProject(LinkBase):
    def __init__(self, project):

        super(DeDeupProject, self).__init__(project)

        self.project_type = 'DEDUP'
        dataset = project['datasets'][0]
        self.left_index = self.right_index = dataset['index_field']
        self.matched = None
        self.out_filename = self.project['name'] + '_dedup_matched.csv'
        self.deduped_filename = self.project['name'] + '_dedup_result.csv'
        self.left_dtypes = self.right_dtypes = None

    def __str__(self):

        descriptor = super(DeDeupProject, self).__str__()

        data_dict = json.loads(descriptor)
        dataset = self.project['datasets'][0]
        data_dict['dataset'] = dataset['name']

        return json.dumps(data_dict, indent=4)

    def _save_linked_data(self, data, append=False):
        data.replace(np.nan, '', regex=True)
        file_path = self.project['output_root'] + self.out_filename
        if not append:
            data.to_csv(file_path)
        else:
            with open(file_path, 'a') as f:
                data.to_csv(f, header=False)

    def load_data(self):

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

        self.right_file = self.left_file = self.project['output_root'] + 'left_file.csv'

        super(DeDeupProject, self).import_data(dataset['url'], usecols, self.left_file, front_cols=[self.left_index])

    def link_pairs(self):
        from cdilinker.linker.union_find import UnionFind
        matched_file = self.output_root + "matched_records.csv"

        link_index = pd.Index([], name='REC_ID')
        left_index = 'LEFT_' + self.left_index
        right_index = 'RIGHT_' + self.right_index

        if not os.path.exists(matched_file) or os.path.getsize(matched_file) == 0:
            return 0

        matched_reader = pd.read_csv(matched_file,
                                     index_col=[left_index, right_index], chunksize=CHUNK_SIZE)

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

        for chunk in matched_reader:
            for left_id, right_id in chunk.index.values:
                entts.union(index_loc[left_id], index_loc[right_id])

        # Assign entity id's
        append = False
        entity_file = self.output_root + 'entity_file.csv'
        matched_reader = pd.read_csv(matched_file,
                                     index_col=[left_index, right_index], chunksize=CHUNK_SIZE)

        for chunk in matched_reader:
            chunk.insert(0, 'ENTITY_ID', np.nan)
            for left_id, right_id in chunk.index.values:
                entt = entts.find(index_loc[left_id])
                entity_ids[entt] = entity_ids[entt] or LinkBase.getNextId()
                chunk.set_value((left_id, right_id), 'ENTITY_ID', entity_ids[entt])
                linked.set_value(left_id, 'ENTITY_ID', entity_ids[entt])
                linked.set_value(right_id, 'ENTITY_ID', entity_ids[entt])

            _save_pairs(entity_file, chunk, append)
            append = True

        os.remove(matched_file)
        os.rename(entity_file, matched_file)

        linked_file = self.output_root + 'entities.csv'
        linked.replace(np.nan, '', regex=True)
        linked.to_csv(linked_file, index=True)

        return entts.count()

    def extract_rows(self, data_filename, data_id, index_filename, index_id, index_cols, selected_filename=None):

        import csv

        if selected_filename is None:
            selected_filename = self.output_root + 'selected_rows.csv'
        remained_filename = self.output_root + 'remained_rows.csv'

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
        os.remove(data_filename)
        os.rename(remained_filename, data_filename)

    def run(self):
        '''
        Runs a de-duplication project consisting of a sequence of steps.
        Each step is defined by a set of blocking and linking identifiers and rules.
        :return: A de-duplicated version of the original data file and the de-duplication summary report.
        '''

        LinkBase.resetId()
        self.steps = {}
        self.linked = pd.DataFrame()

        matched_file = self.output_root + "matched_records.csv"
        selected_filename = self.output_root + 'selected_rows.csv'
        final_selected_file = self.output_root + "final_selected.csv"
        dedup_results_file = self.output_root + self.out_filename

        open(matched_file, 'w').close()

        linked_stats = {}
        prev_total = 0
        self.total_entities = 0
        first_batch = True
        for step in self.project['steps']:
            self.steps[step['seq']] = {}
            print("De-duplication Step {0} :".format(step['seq']))
            print("{0}.1) Finding record pairs satisfying blocking and linking constraints...".format(step['seq']))

            pairs_count = self.pair_n_match(step=step['seq'],
                                            link_method=step['linking_method'],
                                            blocking=step['blocking_schema'],
                                            linking=step['linking_schema'], matched_file=matched_file)

            linked_stats[step['seq']] = pairs_count - prev_total
            prev_total = pairs_count

            if step['group']:
                self.total_entities += self.link_pairs()

                linked_file = self.output_root + 'entities.csv'
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

        os.remove(self.output_root + 'entities.csv')
        os.remove(matched_file)
        os.remove(selected_filename)

    def save(self):
        """
        Create the de-duplicated output file sorted by entity id's and
        Generates the de-duplicated file.
        Preconditions: All de-duplication steps must be completed.
        :return: De-duplication summary report.
        """

        # Adding the selected (de-duped) entities to the final result
        output_filename = self.output_root + self.deduped_filename
        selected_rows = self.output_root + 'final_selected.csv'
        data_reader = pd.read_csv(self.left_file,
                                  usecols=self.left_columns,
                                  skipinitialspace=True, chunksize=CHUNK_SIZE)

        os.rename(selected_rows, output_filename)

        # Assign unique entity id to all remaining records.
        total_remained = 0
        with open(output_filename, 'a') as out_file:
            for chunk in data_reader:
                chunk.insert(0, 'ENTITY_ID', np.nan)
                for rec_id in chunk.index.values:
                    chunk.set_value(rec_id, 'ENTITY_ID', LinkBase.getNextId())
                    total_remained += 1
                chunk.replace(np.nan, '', regex=True)
                chunk.to_csv(out_file, index=False, header=False)

        # Total number of entities after de-duplication
        self.total_entities += total_remained

        # Generating de-duplication summary report
        return generate_linking_summary(self, self.project['output_root'])

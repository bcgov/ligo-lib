from __future__ import print_function

import os
import json
import csv

from .chunked_link_base import LinkBase

from .base import (COLUMN_TYPES,
                   LINKING_RELATIONSHIPS,
                   sort_csv)
from cdilinker.reports.report import generate_linking_summary

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class Linker(LinkBase):
    def __init__(self, project):
        super(Linker, self).__init__(project)
        self.matched_not_linked = None
        self.left_index_type = "object"
        self.right_index_type = "object"

    def __init__(self, project):

        super(Linker, self).__init__(project)

        self.project_type = 'LINK'
        datasets = project['datasets']
        self.left_index = datasets[0]['index_field']
        self.right_index = datasets[1]['index_field']
        self.left_entity = datasets[0]['entity_field']
        self.right_entity = datasets[1]['entity_field']

        self.matched = None
        self.out_filename = self.project['name'] + '_linked_out.csv'
        self.deduped_filename = self.project['name'] + '_dedup_result.csv'

    def __str__(self):

        if self.project is None:
            return ''

        relationship = None
        for rel in LINKING_RELATIONSHIPS:
            if rel[0] == self.project['relationship_type']:
                relationship = rel[1]

        descriptor = super(Linker, self).__str__()

        data_dict = json.loads(descriptor)
        data_dict['datasets'] = [dataset['name'] for dataset in self.project['datasets']]
        data_dict['Relationship_type'] = relationship

        return json.dumps(data_dict, indent=4)

    def load_data(self):

        left_data = self.project['datasets'][0]
        self.left_columns.append(left_data['index_field'])
        self.left_columns.append(left_data['entity_field'])

        if 'data_types' in left_data:
            left_dtypes = {}
            for col_name, col_type in left_data["data_types"].items():
                left_dtypes[col_name] = COLUMN_TYPES[col_type]
        else:
            left_dtypes = None

        try:
            usecols = left_data['columns'] or self.left_columns
        except KeyError:
            usecols = self.left_columns

        self.left_dtypes = left_dtypes
        self.left_columns = usecols

        self.left_file = self.project['output_root'] + 'left_file.csv'
        super(Linker, self).import_data(left_data['url'],
                                        columns=usecols,
                                        dest_filename=self.left_file,
                                        front_cols=[self.left_index, self.left_entity])

        right_data = self.project['datasets'][1]
        self.right_columns.append(right_data['index_field'])
        self.right_columns.append(right_data['entity_field'])

        if 'data_types' in right_data:
            right_dtypes = {}
            for col_name, col_type in right_data["data_types"].items():
                right_dtypes[col_name] = COLUMN_TYPES[col_type]
        else:
            right_dtypes = None

        try:
            usecols = right_data['columns'] or self.right_columns
        except KeyError:
            usecols = self.right_columns

        self.right_dtypes = right_dtypes
        self.right_columns = usecols

        self.right_file = self.project['output_root'] + 'right_file.csv'
        super(Linker, self).import_data(right_data['url'],
                                        usecols,
                                        self.right_file,
                                        front_cols=[self.right_index, self.right_entity])

    def groupby_unique_filter(self, filename, group_col, filter_col,
                              not_linked_filename, add_link_id=True, linked_filename=None):

        stats = {'total_linked': 0, 'total_filtered': 0, 'total_records_linked': 0}

        temp_sorted_file = self.output_root + 'temp_link_file.csv'

        filtered_file = self.output_root + 'temp_link_filtered.csv'
        if add_link_id:
            out_filename = linked_filename
        else:
            out_filename = filtered_file

        # Sort the file based on GroupBy Col and Filter Col
        sort_csv(filename,
                 appendfile=temp_sorted_file,
                 cols=[group_col, filter_col],
                 types={group_col: 'numeric', filter_col: 'numeric'})

        with open(temp_sorted_file, 'r') as in_file, \
                open(out_filename, 'w') as out_file, open(not_linked_filename, 'a') as not_linked_file:
            reader = csv.reader(in_file)
            header = next(reader)
            linked_writer = csv.writer(out_file)
            not_linked_writer = csv.writer(not_linked_file)

            if add_link_id:
                linked_writer.writerow(['LINK_ID'] + header)
            else:
                linked_writer.writerow(header)

            col_index = {key: index for index, key in enumerate(header)}
            group_index = col_index[group_col]
            filter_index = col_index[filter_col]

            buffer = []

            current_group_id = None
            filter = True
            prev_filter_id = None

            for row in reader:
                group_id = row[group_index]

                if group_id != current_group_id:
                    if not filter:
                        if add_link_id:
                            link_id = LinkBase.getNextId()
                            stats['total_linked'] += 1
                        else:
                            link_id = None
                        for item in buffer:
                            if add_link_id:
                                item.insert(0, link_id)
                            stats['total_records_linked'] += 1
                            linked_writer.writerow(item)
                    else:
                        for item in buffer:
                            stats['total_filtered'] += 1
                            not_linked_writer.writerow(item)

                    buffer = []
                    filter = False

                    current_group_id = group_id
                    prev_filter_id = row[filter_index]

                buffer.append(row)
                if prev_filter_id != row[filter_index]:
                    filter = True

            # Write any remaining buffered records
            if len(buffer) > 0:
                if not filter:
                    if add_link_id:
                        link_id = LinkBase.getNextId()
                        stats['total_linked'] += 1
                    else:
                        link_id = None
                    for item in buffer:
                        if add_link_id:
                            item.insert(0, link_id)
                        stats['total_records_linked'] += 1
                        linked_writer.writerow(item)
                else:
                    for item in buffer:
                        stats['total_filtered'] += 1
                        not_linked_writer.writerow(item)

        os.remove(temp_sorted_file)

        return out_filename, stats

    def link(self, step, relationship='1T1'):
        """
        Links the matched record based on relationship type.
        Filters all the record pairs that don't agree on the relationship type.
        :param relationship: Relationship type
            '1T1': One-To-One, default
            '1TN': One-To-Many
            'NT1': Many-To-One
        :return: Linked record pairs.
        """

        matched_file = self.output_root + "matched_records.csv"
        filtered_filename = self.output_root + "filtered_records.csv"
        matched_not_linked_filename = self.output_root + "matched_not_linked_data.csv"
        linked_filename = self.output_root + "step_linked_records.csv"

        group_field = 'RIGHT_' + self.right_entity
        filter_field = 'LEFT_' + self.left_entity

        if relationship == 'MT1':
            group_field, filter_field = filter_field, group_field

        add_link_id = True
        out_filename = linked_filename
        if relationship == '1T1':
            add_link_id = False
            out_filename = None

        temp_filename, stats = self.groupby_unique_filter(matched_file,
                                                          group_col=group_field,
                                                          filter_col=filter_field,
                                                          not_linked_filename=matched_not_linked_filename,
                                                          add_link_id=add_link_id,
                                                          linked_filename=out_filename)

        if relationship == '1T1':
            add_link_id = True
            group_field, filter_field = filter_field, group_field

            os.rename(temp_filename, filtered_filename)
            temp_filename, more_stats = self.groupby_unique_filter(filtered_filename,
                                                                   group_col=group_field,
                                                                   filter_col=filter_field,
                                                                   not_linked_filename=matched_not_linked_filename,
                                                                   add_link_id=add_link_id,
                                                                   linked_filename=linked_filename)

            stats['total_filtered'] += more_stats['total_filtered']
            stats['total_linked'] = more_stats['total_linked']
            stats['total_records_linked'] = more_stats['total_records_linked']

            os.remove(filtered_filename)

        return stats

    def extract_linked_records(self, linked_filename, prefix='LEFT_'):

        def add_data_row_to_linked(row, link_id, writer):

            extra_row = [None] * len(linked_header)
            for index, value in enumerate(row):
                col_name = data_header[index]
                extra_row[linked_map[prefix + col_name]] = value
            extra_row[linked_map['LINK_ID']] = link_id

            writer.writerow(extra_row)

        temp_linked_file = self.output_root + "temp_link_sorted.csv"
        temp_data_file = self.output_root + "temp_data.csv"

        if prefix == 'LEFT_':
            entity_field = self.left_entity
            index_filed = self.left_index
            data_filename = self.left_file
        else:
            entity_field = self.right_entity
            index_filed = self.right_index
            data_filename = self.right_file

        linked_entity_col = prefix + entity_field
        linked_index_col = prefix + index_filed

        sort_csv(linked_filename,
                 appendfile=temp_linked_file,
                 cols=[linked_entity_col, linked_index_col],
                 types={linked_entity_col: 'numeric', linked_index_col: 'numeric'})

        with open(temp_linked_file, 'r') as linked_file, open(data_filename, 'r') as data_file, \
                open(linked_filename, 'w') as link_out, open(temp_data_file, 'w') as data_out:

            linked_reader = csv.reader(linked_file)
            data_reader = csv.reader(data_file)

            linked_writer = csv.writer(link_out)
            data_writer = csv.writer(data_out)

            # Get header rows
            linked_header = next(linked_reader)
            data_header = next(data_reader)

            # Add header rows to the output files.
            linked_writer.writerow(linked_header)
            data_writer.writerow(data_header)

            # Get column name to column index mapping
            linked_map = {key: index for index, key in enumerate(linked_header)}
            link_entity_index = linked_map[linked_entity_col]  # Link Entity column index
            link_ingestion_index = linked_map[linked_index_col]  # Link ingestion column index

            data_map = {key: index for index, key in enumerate(data_header)}
            data_entity_index = data_map[entity_field]
            data_ingestion_index = data_map[index_filed]

            previous_link_row = None

            try:
                linked_row = next(linked_reader)
                data_row = next(data_reader)

                while True:

                    if previous_link_row and \
                            float(previous_link_row[link_entity_index]) == float(linked_row[link_entity_index]) and \
                            float(previous_link_row[link_ingestion_index]) == float(linked_row[link_ingestion_index]):

                        linked_writer.writerow(linked_row)
                        linked_row = next(linked_reader)


                    elif float(data_row[data_entity_index]) < float(linked_row[link_entity_index]):
                        if previous_link_row and \
                                float(previous_link_row[link_entity_index]) == float(data_row[data_entity_index]):
                            add_data_row_to_linked(data_row,
                                                   previous_link_row[linked_map['LINK_ID']],
                                                   linked_writer)
                        else:
                            data_writer.writerow(data_row)


                        data_row = next(data_reader)

                    else:
                        if float(data_row[data_ingestion_index]) < float(linked_row[link_ingestion_index]):
                            add_data_row_to_linked(data_row,
                                                   linked_row[linked_map['LINK_ID']],
                                                   linked_writer)
                        else:
                            linked_writer.writerow(linked_row)
                            previous_link_row = linked_row
                            linked_row = next(linked_reader)
                        data_row = next(data_reader)
            except StopIteration:
                pass

            for data_row in data_reader:
                if previous_link_row and \
                        float(previous_link_row[link_entity_index]) == float(data_row[data_entity_index]):
                    add_data_row_to_linked(data_row,
                                           previous_link_row[linked_map['LINK_ID']], linked_writer)
                else:
                    data_writer.writerow(data_row)
                    previous_link_row = None

        os.remove(data_filename)
        os.rename(temp_data_file, data_filename)

        os.remove(linked_filename)
        os.rename(temp_linked_file, linked_filename)

    def run(self):

        self.steps = {}
        linked_stats = {}
        prev_total = 0
        self.total_entities = 0
        self.total_records_linked = 0

        matched_file = self.output_root + "matched_records.csv"

        matched_not_linked_filename = self.output_root + "matched_not_linked_data.csv"
        linked_filename = self.output_root + "linked_records.csv"
        step_linked = self.output_root + "step_linked_records.csv"
        temp_sorted_file = self.output_root + "temp_sort_data.csv"

        open(linked_filename, 'w').close()

        open(matched_not_linked_filename, 'w').close()

        first_batch = True

        for step in self.project['steps']:
            # Sort input files based on entity_id and ingestion id:
            sort_csv(self.left_file,
                     appendfile=temp_sorted_file,
                     cols=[self.left_index],
                     types={self.left_index: 'numeric'})
            os.remove(self.left_file)
            os.rename(temp_sorted_file, self.left_file)
            sort_csv(self.right_file,
                     appendfile=temp_sorted_file,
                     cols=[self.right_index],
                     types={self.right_index: 'numeric'})
            os.remove(self.right_file)
            os.rename(temp_sorted_file, self.right_file)

            self.steps[step['seq']] = {}
            print("Linking Step {0} :".format(step['seq']))
            print("{0}.1) Finding record pairs satisfying blocking and linking constraints...".format(step['seq']))

            open(matched_file, 'w').close()
            pairs_count = self.pair_n_match(step=step['seq'],
                                                    link_method=step['linking_method'],
                                                    blocking=step['blocking_schema'],
                                                    linking=step['linking_schema'], matched_file=matched_file)

            linked_stats[step['seq']] = pairs_count

            print ("{0}.3) Identifying the linked records based on the relationship type...".format(step['seq']))
            link_stats = self.link(self.project['relationship_type'])

            self.steps[step['seq']]['total_records_linked'] = link_stats['total_records_linked']
            self.total_records_linked += link_stats['total_records_linked']
            self.steps[step['seq']]['total_matched_not_linked'] = link_stats['total_filtered']
            self.steps[step['seq']]['total_entities'] = link_stats['total_linked']
            self.total_entities += self.steps[step['seq']]['total_entities']

            # Sort input files based on entity_id and ingestion id:
            sort_csv(self.left_file,
                     appendfile=temp_sorted_file,
                     cols=[self.left_entity, self.left_index],
                     types={self.left_entity: 'numeric', self.left_index: 'numeric'})
            os.remove(self.left_file)
            os.rename(temp_sorted_file, self.left_file)
            sort_csv(self.right_file,
                     appendfile=temp_sorted_file,
                     cols=[self.right_entity, self.right_index],
                     types={self.right_entity: 'numeric', self.right_index: 'numeric'})
            os.remove(self.right_file)
            os.rename(temp_sorted_file, self.right_file)

            self.extract_linked_records(linked_filename=step_linked, prefix='LEFT_')
            self.extract_linked_records(linked_filename=step_linked, prefix='RIGHT_')

            LinkBase.append_rows(linked_filename, step_linked, first_batch=first_batch)
            first_batch = False

    def save(self):
        print ("Writing results to the output files ...")
        linked_file_path = self.project['output_root'] + "linked_data.csv"

        linked_filename = self.output_root + "linked_records.csv"
        temp_sorted_file = self.output_root + "temp_sort_data.csv"

        sort_csv(linked_filename,
                 appendfile=temp_sorted_file,
                 cols=['LINK_ID'],
                 types={'LINK_ID': 'numeric'})

        os.remove(linked_filename)
        os.rename(temp_sorted_file, linked_file_path)

        print ('Sorting remaining records...')
        sort_csv(self.left_file,
                 appendfile=temp_sorted_file,
                 cols=[self.left_index],
                 types={self.left_index: 'numeric'})
        os.remove(self.left_file)
        os.rename(temp_sorted_file, self.left_file)
        sort_csv(self.right_file,
                 appendfile=temp_sorted_file,
                 cols=[self.right_index],
                 types={self.right_index: 'numeric'})
        os.remove(self.right_file)
        os.rename(temp_sorted_file, self.right_file)

        return generate_linking_summary(self, self.project['output_root'])

import os
import json
import csv
import logging
import shutil


from .base import (link_config,
                                   COLUMN_TYPES,
                                   LINKING_RELATIONSHIPS,
                                   sort_csv)
from .chunked_link_base import ChunkedLinkBase
from .files import LinkFiles
from ..reports.report import generate_linking_summary


logger = logging.getLogger(__name__)


class ChunkedLink(ChunkedLinkBase):
    def __init__(self, project):
        if project is None:
            raise TypeError
        super(ChunkedLink, self).__init__(project)
        self.matched_not_linked = None

        self.project_type = 'LINK'
        datasets = project['datasets']
        self.left_index = datasets[0]['index_field']
        self.right_index = datasets[1]['index_field']
        self.left_entity = datasets[0]['entity_field']
        self.right_entity = datasets[1]['entity_field']

        self.matched = None

    def __str__(self):
        if self.project is None:
            return ''

        relationship = None
        for rel in LINKING_RELATIONSHIPS:
            if rel[0] == self.project['relationship_type']:
                relationship = rel[1]

        descriptor = super(ChunkedLink, self).__str__()

        data_dict = json.loads(descriptor)
        data_dict['datasets'] = [dataset['name'] for dataset in self.project['datasets']]
        data_dict['Relationship_type'] = relationship

        return json.dumps(data_dict, indent=4)

    def load_data(self):
        logger.debug('>>--- load_data --->>')
        logger.info('Loading input datasets for project: %s with task id: %s.',
                    self.project['name'], self.project['task_uuid'])

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

        logger.debug('Left data columns: %s.', self.left_columns)
        logger.debug('Left data types: %s', self.left_dtypes)

        self.left_file = self.output_root + \
            link_config.get('left_file', 'left_file.csv')

        super(ChunkedLink, self).import_data(left_data['url'],
                                             columns=usecols,
                                             dest_filename=self.left_file,
                                             front_cols=[self.left_index,
                                                         self.left_entity],
                                             data_types=self.left_dtypes)

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

        logger.debug('Right data columns: %s.', self.right_columns)
        logger.debug('Right data types: %s', self.right_dtypes)

        self.right_file = self.output_root + \
            link_config.get('right_file', 'right_file.csv')

        super(ChunkedLink, self).import_data(right_data['url'],
                                             usecols,
                                             self.right_file,
                                             front_cols=[self.right_index,
                                                         self.right_entity],
                                             data_types=self.right_dtypes)

        logger.debug('<<--- load_data ---<<')

    def groupby_unique_filter(self, filename, group_col, filter_col,
                              not_linked_filename, add_link_id=True, linked_filename=None):
        logger.debug('>>--- groupby_unique_filter --->>')
        stats = {'total_linked': 0, 'total_filtered': 0, 'total_records_linked': 0}

        temp_sorted_file = self.temp_path + LinkFiles.TEMP_LINK_FILE

        filtered_file = self.temp_path + LinkFiles.TEMP_LINK_FILTERED
        if add_link_id:
            out_filename = linked_filename
        else:
            out_filename = filtered_file

        # Sort the file based on GroupBy Col and Filter Col
        sort_csv(filename,
                 appendfile=temp_sorted_file,
                 cols=[group_col, filter_col],
                 types={group_col: 'numeric', filter_col: 'numeric'},
                 work_dir=self.temp_path)

        with open(temp_sorted_file, 'r') as in_file, \
                open(out_filename, 'w') as out_file, \
                open(not_linked_filename, 'a') as not_linked_file:
            reader = csv.reader(in_file)
            header = next(reader)
            linked_writer = csv.writer(out_file, lineterminator='\n')
            not_linked_writer = csv.writer(not_linked_file, lineterminator='\n')

            if add_link_id:
                logger.info('Assigning link id to the selected subset of record pairs.')
                linked_writer.writerow(['LINK_ID'] + header)
            else:
                linked_writer.writerow(header)

            col_index = {key: index for index, key in enumerate(header)}
            group_index = col_index[group_col]
            filter_index = col_index[filter_col]

            buffer = []

            current_group_id = None
            current_filter = True
            prev_filter_id = None

            for row in reader:
                group_id = row[group_index]

                if group_id != current_group_id:
                    if not current_filter:
                        if add_link_id:
                            link_id = ChunkedLinkBase.get_next_id()
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
                    current_filter = False

                    current_group_id = group_id
                    prev_filter_id = row[filter_index]

                buffer.append(row)
                if prev_filter_id != row[filter_index]:
                    current_filter = True

            # Write any remaining buffered records
            if buffer:
                if not current_filter:
                    if add_link_id:
                        link_id = ChunkedLinkBase.get_next_id()
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

        if os.path.isfile(temp_sorted_file):
            os.remove(temp_sorted_file)

        logger.debug('<<--- groupby_unique_filter ---<<')
        return out_filename, stats

    def link(self, relationship='1T1'):
        """
        Links the matched record based on relationship type.
        Filters all the record pairs that don't agree on the relationship type.
        :param relationship: Relationship type
            '1T1': One-To-One, default
            '1TN': One-To-Many
            'NT1': Many-To-One
        :return: Linked record pairs.
        """
        logger.debug('>>--- link --->>')
        logger.info('Linking the records pairs based on the relationship type.')
        matched_file = self.temp_path + LinkFiles.MATCHED_RECORDS
        filtered_filename = self.temp_path + LinkFiles.TEMP_FILTER_RECORDS

        matched_not_linked_filename = self.output_root + link_config \
            .get('matched_not_linked_filename', 'matched_not_linked_data.csv')

        linked_filename = self.temp_path + LinkFiles.TEMP_STEP_LINKED_FILE

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

            if os.path.isfile(temp_filename):
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

            if os.path.isfile(filtered_filename):
                os.remove(filtered_filename)

        logger.debug('<<--- link ---<<')
        return stats

    def extract_linked_records(self, linked_filename, prefix='LEFT_'):
        logger.debug('>>--- extract_linked_records --->>')
        logger.info('Removing all linked records from the input data file and preparing data for the next step.')

        def add_data_row_to_linked(row, link_id, writer):

            extra_row = [None] * len(linked_header)
            for index, value in enumerate(row):
                col_name = data_header[index]
                extra_row[linked_map[prefix + col_name]] = value
            extra_row[linked_map['LINK_ID']] = link_id

            writer.writerow(extra_row)

        temp_linked_file = self.temp_path + LinkFiles.TEMP_LINK_SORTED
        temp_data_file = self.temp_path + LinkFiles.TEMP_LINKING_DATA

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
                 types={linked_entity_col: 'numeric', linked_index_col: 'numeric'},
                 work_dir=self.temp_path)

        with open(temp_linked_file, 'r') as linked_file, \
                open(data_filename, 'r') as data_file, \
                open(linked_filename, 'w') as link_out, \
                open(temp_data_file, 'w') as data_out:

            linked_reader = csv.reader(linked_file)
            data_reader = csv.reader(data_file)

            linked_writer = csv.writer(link_out, lineterminator='\n')
            data_writer = csv.writer(data_out, lineterminator='\n')

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

        if os.path.isfile(data_filename):
            os.remove(data_filename)
        if os.path.isfile(temp_data_file):
            os.rename(temp_data_file, data_filename)

        if os.path.isfile(linked_filename):
            os.remove(linked_filename)
        if os.path.isfile(temp_linked_file):
            os.rename(temp_linked_file, linked_filename)

        logger.debug('<<--- extract_linked_records ---<<')

    def run(self):
        logger.debug('>>--- run --->>')
        logger.info('Executing linking project %s. Task id: %s.',
                    self.project['name'], self.project['task_uuid'])

        ChunkedLinkBase.reset_id()
        self.steps = {}
        linked_stats = {}
        self.total_entities = 0
        self.total_records_linked = 0

        matched_file = self.temp_path + LinkFiles.MATCHED_RECORDS

        matched_not_linked_filename = self.output_root \
            + link_config.get('matched_not_linked_filename', 'matched_not_linked_data.csv')
        linked_filename = self.temp_path + LinkFiles.TEMP_LINKED_RECORDS
        step_linked = self.temp_path + LinkFiles.TEMP_STEP_LINKED_FILE
        temp_sorted_file = self.temp_path + LinkFiles.TEMP_SORTED_FILE

        open(linked_filename, 'w').close()
        open(matched_not_linked_filename, 'w').close()

        first_batch = True

        for step in self.project['steps']:
            # Sort input files based on entity_id and ingestion id:
            sort_csv(self.left_file,
                     appendfile=temp_sorted_file,
                     cols=[self.left_index],
                     types={self.left_index: 'numeric'},
                     work_dir=self.temp_path)

            os.remove(self.left_file)
            os.rename(temp_sorted_file, self.left_file)
            sort_csv(self.right_file,
                     appendfile=temp_sorted_file,
                     cols=[self.right_index],
                     types={self.right_index: 'numeric'},
                     work_dir=self.temp_path)
            os.remove(self.right_file)
            os.rename(temp_sorted_file, self.right_file)

            self.steps[step['seq']] = {}
            logger.info("Linking Step %s :", step['seq'])
            logger.info("%s.1) Finding record pairs satisfying blocking and linking constraints...",
                        step['seq'])

            open(matched_file, 'w').close()
            pairs_count = self.pair_n_match(step=step['seq'],
                                            link_method=step['linking_method'],
                                            blocking=step['blocking_schema'],
                                            linking=step['linking_schema'],
                                            matched_file=matched_file)

            linked_stats[step['seq']] = pairs_count

            if pairs_count == 0:
                logger.info('No records matched at step %s', step['seq'])
                self.steps[step['seq']]['total_records_linked'] = 0
                self.steps[step['seq']]['total_matched_not_linked'] = 0
                self.steps[step['seq']]['total_entities'] = 0
                continue

            logger.info("%s.3) Identifying the linked records based on the relationship type...",
                        step['seq'])
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
                     types={self.left_entity: 'numeric', self.left_index: 'numeric'},
                     work_dir=self.temp_path)
            os.remove(self.left_file)
            os.rename(temp_sorted_file, self.left_file)
            sort_csv(self.right_file,
                     appendfile=temp_sorted_file,
                     cols=[self.right_entity, self.right_index],
                     types={self.right_entity: 'numeric', self.right_index: 'numeric'},
                     work_dir=self.temp_path)
            os.remove(self.right_file)
            os.rename(temp_sorted_file, self.right_file)

            self.extract_linked_records(linked_filename=step_linked, prefix='LEFT_')
            self.extract_linked_records(linked_filename=step_linked, prefix='RIGHT_')

            ChunkedLinkBase.append_rows(linked_filename, step_linked, first_batch=first_batch)
            first_batch = False

        if os.path.isfile(step_linked):
            os.remove(step_linked)
        if os.path.isfile(matched_file):
            os.remove(matched_file)

        logger.info('Execution of linking project %s with Task id: %s is completed.',
                    self.project['name'], self.project['task_uuid'])
        logger.debug('<<--- run ---<<')

    def save(self):
        logger.debug('>>--- save --->>')
        logger.info("Preparing output file of the linking project %s with tsk id %s.",
                    self.project['name'], self.project['task_uuid'])

        linked_file_path = self.output_root + link_config.get('linked_data_file', 'linked_data.csv')

        linked_filename = self.temp_path + LinkFiles.TEMP_LINKED_RECORDS
        temp_sorted_file = self.temp_path + LinkFiles.TEMP_SORTED_FILE

        if self.total_records_linked > 0:
            sort_csv(linked_filename,
                     appendfile=temp_sorted_file,
                     cols=['LINK_ID'],
                     types={'LINK_ID': 'numeric'},
                     work_dir=self.temp_path)
            if os.path.isfile(temp_sorted_file):
                os.rename(temp_sorted_file, linked_file_path)

        if os.path.isfile(linked_filename):
            os.remove(linked_filename)

        sort_csv(self.left_file,
                 appendfile=temp_sorted_file,
                 cols=[self.left_index],
                 types={self.left_index: 'numeric'},
                 work_dir=self.temp_path)

        if os.path.isfile(self.left_file):
            os.remove(self.left_file)
        if os.path.isfile(temp_sorted_file):
            os.rename(temp_sorted_file, self.left_file)

        sort_csv(self.right_file,
                 appendfile=temp_sorted_file,
                 cols=[self.right_index],
                 types={self.right_index: 'numeric'},
                 work_dir=self.temp_path)

        if os.path.isfile(self.right_file):
            os.remove(self.right_file)
        if os.path.isfile(temp_sorted_file):
            os.rename(temp_sorted_file, self.right_file)

        # Clean all remaining temp files
        if os.path.exists(self.temp_path):
            shutil.rmtree(self.temp_path)

        logger.info('Linking output file generated at %s.', linked_file_path)
        logger.debug('<<--- save ---<<')

        return generate_linking_summary(self, self.output_root)

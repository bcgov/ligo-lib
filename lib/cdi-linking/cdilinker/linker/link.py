import os
import json
import pandas as pd
import numpy as np

from cdilinker.linker.link_base import LinkBase
from cdilinker.reports.report import generate_linking_summary
from .base import (link_config,
                   COLUMN_TYPES,
                   LINKING_RELATIONSHIPS)

from cdilinker.linker.files import LinkFiles

import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class Linker(LinkBase):
    def __init__(self, project):
        if project is None:
            raise TypeError
        super(Linker, self).__init__(project)
        self.matched_not_linked = None
        self.left_index_type = "object"
        self.right_index_type = "object"

        self.project_type = 'LINK'
        datasets = project['datasets']
        self.left_index = datasets[0]['index_field']
        self.right_index = datasets[1]['index_field']
        self.left_entity = datasets[0]['entity_field']
        self.right_entity = datasets[1]['entity_field']

    def __str__(self):

        if self.project is None:
            return ''

        relationship = None
        for rel in LINKING_RELATIONSHIPS:
            if rel[0] == self.project['relationship_type']:
                relationship = rel[1]

        descriptor = super(Linker, self).__str__()

        data_dict = json.loads(descriptor)
        data_dict['datasets'] = [dataset['name'] for dataset in
                                 self.project['datasets']]
        data_dict['Relationship_type'] = relationship

        return json.dumps(data_dict, indent=4)

    def load_data(self):
        logger.debug('>>--- load_data --->>')
        logger.info('Loading input dataset for project: {0} with task id: {1}.'
                    .format(self.project['name'], self.project['task_uuid']))

        super(Linker, self).load_data()
        datasets = self.project['datasets']
        if datasets and len(datasets) > 1:

            self.left_columns += [datasets[0]['index_field'],
                                  datasets[0]['entity_field']]
            self.right_columns += [datasets[1]['index_field'],
                                   datasets[1]['entity_field']]

            if 'data_types' in datasets[0]:
                left_dtypes = {}
                for col_name, col_type in datasets[0]["data_types"].items():
                    left_dtypes[col_name] = COLUMN_TYPES[col_type]
                if self.left_index in left_dtypes:
                    self.left_index_type = left_dtypes[self.left_index]
            else:
                left_dtypes = None

            if 'data_types' in datasets[1]:
                right_dtypes = {}
                for col_name, col_type in datasets[1]["data_types"].items():
                    right_dtypes[col_name] = COLUMN_TYPES[col_type]
                if self.right_index in right_dtypes:
                    self.right_index_type = right_dtypes[self.right_index]
            else:
                right_dtypes = None

        try:
            left_usecols = datasets[0]['columns'] or self.left_columns
        except KeyError:
            left_usecols = self.left_columns

        logger.debug('Left columns: {0}.'.format(left_usecols))
        logger.debug('Left index: {0}'.format(self.left_index))
        logger.debug('Left data types: {0}'.format(left_dtypes))
        self.left_dataset = pd.read_csv(datasets[0]['url'],
                                        index_col=self.left_index,
                                        usecols=left_usecols,
                                        skipinitialspace=True,
                                        dtype=left_dtypes)

        try:
            right_usecols = datasets[1]['columns'] or self.right_columns
        except KeyError:
            right_usecols = self.right_columns

        logger.debug('Right columns: {0}.'.format(right_usecols))
        logger.debug('Right index: {0}'.format(self.right_index))
        logger.debug('Right data types: {0}'.format(right_dtypes))
        self.right_dataset = pd.read_csv(datasets[1]['url'],
                                         index_col=self.right_index,
                                         usecols=right_usecols,
                                         skipinitialspace=True,
                                         dtype=right_dtypes)

        logger.debug('<<--- load_data ---<<')

    def link(self, seq, relationship='1T1'):
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
        match_file_path = self.output_root + LinkFiles.TEMP_MATCHED_FILE
        matched = pd.read_csv(match_file_path,
                              index_col=['LEFT_' + self.left_index,
                                         'RIGHT_' + self.right_index])

        group_field = 'RIGHT_' + self.right_entity
        filter_field = 'LEFT_' + self.left_entity
        if relationship == 'MT1':
            group_field, filter_field = filter_field, group_field

        relationship_group = matched.groupby(group_field)
        linked = relationship_group.filter(
            lambda x: len(x[filter_field].unique()) == 1)

        if relationship == '1T1':
            group_field, filter_field = filter_field, group_field

            relationship_group = linked.groupby(group_field)
            linked = relationship_group.filter(
                lambda x: len(x[filter_field].unique()) == 1)

        linked['STEP'] = seq

        logger.info('Assigning link id to the selected subset of record pairs.')
        left_entity_id = 'LEFT_' + self.left_entity
        right_entity_id = 'RIGHT_' + self.right_entity
        link_index = linked.reset_index()[
            [left_entity_id, right_entity_id]].drop_duplicates()
        link_index = link_index.set_index([left_entity_id, right_entity_id])
        link_index['LINK_ID'] = pd.Series(
            [LinkBase.get_next_id() for row in link_index.index],
            index=link_index.index)
        link_index['LINK_ID'] = link_index['LINK_ID'].map(
            lambda x: '{:.0f}'.format(x)
            if pd.notnull(x)
            else np.nan)
        linked = linked.join(link_index,
                             on=[left_entity_id, right_entity_id],
                             how='inner')
        matched_not_linked = self.get_rows_not_in(matched, linked.index)
        matched_not_linked['STEP'] = seq

        logger.debug('<<--- link ---<<')
        return linked, matched_not_linked

    def run(self):

        logger.debug('>>--- run --->>')

        logger.info('Executing linking project {0}. Task id: {1}.'
                    .format(self.project['name'], self.project['task_uuid']))

        self.steps = {}
        self.total_records_linked = 0
        self.total_entities = 0
        logger.info('Project steps: {0}'.format(len(self.project['steps'])))

        LinkBase.reset_id()
        for step in self.project['steps']:
            self.steps[step['seq']] = {}
            logger.info("Linking Step {0} :".format(step['seq']))
            logger.info("{0}.1) Finding record pairs satisfying blocking constraints...".format(step['seq']))
            self.pair_n_match(step=step['seq'],
                              link_method=step['linking_method'],
                              blocking=step['blocking_schema'],
                              linking=step['linking_schema'])

            logger.info("{0}.2) Identifying the linked records based on the relationship type...".format(step['seq']))
            step_linked, step_matched_not_linked = self.link(step['seq'], self.project['relationship_type'])

            left_index = 'LEFT_' + self.left_index
            right_index = 'RIGHT_' + self.right_index
            left_entity_id = 'LEFT_' + self.left_entity
            right_entity_id = 'RIGHT_' + self.right_entity
            self.steps[step['seq']]['total_records_linked'] = len(
                step_linked.index.values)
            self.total_records_linked += len(step_linked.index.values)
            self.steps[step['seq']]['total_entities'] = len(
                step_linked.groupby([left_entity_id, right_entity_id]))
            self.total_entities += self.steps[step['seq']]['total_entities']

            if not step_linked.empty:
                # Cretae EntityID - LinkId map
                left_links = step_linked[
                    [left_entity_id, 'LINK_ID']].drop_duplicates()
                left_links = left_links.reset_index().set_index(
                    left_entity_id)['LINK_ID']
                left_match = self.left_dataset.join(left_links,
                                                    on=self.left_entity,
                                                    how='inner')

                linked = pd.merge(
                    left_match.reset_index(),
                    step_linked.reset_index(),
                    left_on=self.left_index,
                    right_on=left_index,
                    how='left'
                )
                linked.drop(left_index, axis=1, inplace=True)
                linked.drop(left_entity_id, axis=1, inplace=True)
                linked.rename(columns={self.left_index: left_index},
                              inplace=True)
                linked.rename(columns={self.left_entity: left_entity_id},
                              inplace=True)

                right_links = step_linked[
                    [right_entity_id, 'LINK_ID']].drop_duplicates()
                right_links = right_links.reset_index().set_index(
                    right_entity_id)['LINK_ID']
                right_match = self.right_dataset.join(right_links,
                                                      on=self.right_entity,
                                                      how='inner')

                linked = pd.merge(
                    linked,
                    right_match.reset_index(),
                    left_on=right_index,
                    right_on=self.right_index,
                    how='right'
                )

                linked.drop(right_index, axis=1, inplace=True)
                linked.drop(right_entity_id, axis=1, inplace=True)
                linked.rename(columns={self.right_index: right_index},
                              inplace=True)
                linked.rename(columns={self.right_entity: right_entity_id},
                              inplace=True)
                linked.drop(['LINK_ID_x', 'LINK_ID_y'], axis=1, inplace=True)
            else:
                linked = pd.DataFrame()

            self.linked = linked if self.linked is None \
                else self.linked.append(linked)

            self.steps[step['seq']]['total_matched_not_linked'] = len(
                step_matched_not_linked.index.values)
            if self.matched_not_linked is None:
                self.matched_not_linked = step_matched_not_linked
            else:
                self.matched_not_linked = self.matched_not_linked.append(
                    step_matched_not_linked)

            self.left_dataset = self.left_dataset[
                ~self.left_dataset[self.left_entity].isin(
                    step_linked[left_entity_id])]
            self.right_dataset = self.right_dataset[
                ~self.right_dataset[self.right_entity].isin(
                    step_linked[right_entity_id])]

            logger.info("Number of records linked at step {0}: {1}".format(step['seq'], len(self.linked)))

        temp_match_file_path = self.output_root + LinkFiles.TEMP_MATCHED_FILE
        # Delete temporary matched file.
        if os.path.isfile(temp_match_file_path):
            os.remove(temp_match_file_path)

        logger.info('Execution of linking project {0} with Task id: {1} is completed.'
                    .format(self.project['name'], self.project['task_uuid']))
        logger.debug('<<--- run ---<<')

    def save(self):
        logger.debug('>>--- save --->>')

        left_index = 'LEFT_' + self.left_index
        right_index = 'RIGHT_' + self.right_index
        left_entity_id = 'LEFT_' + self.left_entity
        right_entity_id = 'RIGHT_' + self.right_entity
        grouped = self.matched_not_linked.reset_index().groupby([
            left_index, right_index, left_entity_id, right_entity_id]).agg(
            {'STEP': 'min'})

        self.matched_not_linked = pd.DataFrame(grouped)

        # Storing linked data records.
        logger.info("Preparing output files of the linking project {0} with tsk id {1}."
                    .format(self.project['name'], self.project['task_uuid']))
        linked_file_path = self.project['output_root'] + link_config.get('linked_data_file', 'linked_data.csv')

        self.linked['STEP'] = self.linked['STEP'].map(
            lambda x: '{:.0f}'.format(x)
            if pd.notnull(x)
            else np.nan)

        self.linked[right_entity_id] = self.linked[right_entity_id].map(
            lambda x: '{:.0f}'.format(x)
            if pd.notnull(x)
            else np.nan)

        self.linked[left_entity_id] = self.linked[left_entity_id].map(
            lambda x: '{:.0f}'.format(x)
            if pd.notnull(x)
            else np.nan)

        if np.issubdtype(self.left_index_type, np.integer):
            self.linked[left_index] = self.linked[left_index].map(
                lambda x: '{:.0f}'.format(x)
                if pd.notnull(x)
                else np.nan)

        if np.issubdtype(self.right_index_type, np.integer):
            self.linked[right_index] = self.linked[right_index].map(
                lambda x: '{:.0f}'.format(x)
                if pd.notnull(x)
                else np.nan)

        self.linked = self.linked.sort_values(['LINK_ID'])
        self.linked.replace(np.nan, '', regex=True)
        self.linked.to_csv(linked_file_path, index=False)

        # Storing matched but not linked records.
        matched_file_path = self.project['output_root'] + link_config.get(
            'matched_not_linked_filename', 'matched_not_linked_data.csv')
        self.matched_not_linked.replace(np.nan, '', regex=True)
        self.matched_not_linked.to_csv(matched_file_path)

        logger.info('Linking output files generated: {0},\n {1}.'.format(linked_file_path, matched_file_path))
        logger.debug('<<--- save ---<<')
        return generate_linking_summary(self, self.project['output_root'])

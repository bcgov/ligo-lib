import json
import os
import pandas as pd
import numpy as np

from cdilinker.linker.link_base import LinkBase
from cdilinker.reports.report import generate_linking_summary
from .base import (link_config, COLUMN_TYPES)
from cdilinker.linker.files import LinkFiles

import logging

logger = logging.getLogger(__name__)


class DeDeupProject(LinkBase):

    def __init__(self, project):
        if project is None:
            raise TypeError
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

    def save_linked_data(self, data, append=False):
        logger.debug('>>--- save_linked_data --->>')
        file_path = self.project['output_root'] + link_config.get(
            'dedup_matched_file', 'dedup_matched.csv')

        data.replace(np.nan, '', regex=True)
        if not append:
            data.to_csv(file_path)
        else:
            with open(file_path, 'a') as f:
                data.to_csv(f, header=False)

        logger.debug('<<--- save_linked_data ---<<')

    def load_data(self):
        logger.debug('>>--- load_data --->>')
        logger.info('Loading input dataset for project: {0} with task id: {1}.'
                    .format(self.project['name'], self.project['task_uuid']))
        super(DeDeupProject, self).load_data()
        if self.project['datasets'] and len(self.project['datasets']) > 0:
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

            self.left_dtypes = left_dtypes
            self.left_dataset = pd.read_csv(dataset['url'],
                                            index_col=dataset['index_field'],
                                            usecols=usecols,
                                            skipinitialspace=True,
                                            dtype=self.left_dtypes)
        logger.debug('<<--- load_data ---<<')

    def link(self):
        logger.debug('>>--- link --->>')
        from .union_find import UnionFind

        logger.info('Assigning entity id to linked records.')
        left_index = self.matched.index.get_level_values(0).drop_duplicates()
        right_index = self.matched.index.get_level_values(1).drop_duplicates()
        link_index = left_index.union(right_index)
        link_index = link_index.rename('REC_ID')

        # Create index to location map
        rec_len = len(link_index)
        index_loc = pd.Series(list(range(rec_len)), index=link_index)
        entity_ids = [None] * rec_len

        # Create disjoint set of entities
        entts = UnionFind(rec_len)

        linked = pd.DataFrame(columns=['ENTITY_ID'], index=link_index)
        linked['ENTITY_ID'].fillna(0, inplace=True)
        linked['ENTITY_ID'] = linked['ENTITY_ID'].map(
            lambda x: '{:.0f}'.format(x)
            if pd.notnull(x)
            else np.nan)

        # Find all connected records and make entity groups
        for left_id, right_id in self.matched.index.values:
            entts.union(index_loc[left_id], index_loc[right_id])

        self.matched.insert(0, 'ENTITY_ID', np.nan)
        # Assign entity id's
        for left_id, right_id in self.matched.index.values:
            entt = entts.find(index_loc[left_id])
            entity_ids[entt] = entity_ids[entt] or LinkBase.get_next_id()
            self.matched.set_value((left_id, right_id), 'ENTITY_ID',
                                   entity_ids[entt])
            linked.set_value(left_id, 'ENTITY_ID', entity_ids[entt])
            linked.set_value(right_id, 'ENTITY_ID', entity_ids[entt])

        logger.debug('<<--- link ---<<')
        return linked

    def run(self):
        logger.debug('>>--- run --->>')

        logger.info('Executing de-duplication project {0}. Task id: {1}.'
                    .format(self.project['name'], self.project['task_uuid']))
        append = False

        LinkBase.reset_id()

        self.steps = {}
        self.linked = pd.DataFrame()
        # total_step_entities = None # Unused
        for step in self.project['steps']:
            self.steps[step['seq']] = {}
            logger.info("De-duplication Step {0} :".format(step['seq']))
            logger.info("{0}.1) Finding record pairs satisfying blocking constraints...".format(step['seq']))
            self.pair_n_match(step=step['seq'],
                              link_method=step['linking_method'],
                              blocking=step['blocking_schema'],
                              linking=step['linking_schema'])

            match_file_path = \
                self.project['output_root'] + LinkFiles.TEMP_MATCHED_FILE

            left_index = 'LEFT_' + self.left_index
            right_index = 'RIGHT_' + self.right_index
            if os.path.isfile(match_file_path):
                matched = pd.read_csv(match_file_path,
                                      index_col=[left_index, right_index])
                self.matched = matched \
                    if self.matched is None else self.matched.append(matched)
                self.matched = self.matched.groupby(level=[0, 1]).min()
                self.matched = pd.DataFrame(self.matched)

            if step['group'] and self.matched is not None \
                    and not self.matched.empty:
                self.total_records_linked += len(self.matched.index)
                # Group rows that blong to the same entity and assign entity id
                result = self.link()
                step_group = self.matched.groupby(['STEP'])

                total_linked = step_group.size()

                self.total_linked = total_linked \
                    if self.total_linked is None \
                    else self.total_linked.append(total_linked)

                left_cols = self.get_rows_in(
                    self.left_dataset,
                    self.matched.index.get_level_values(0)
                ).reset_index().drop_duplicates()

                right_cols = self.get_rows_in(
                    self.left_dataset,
                    self.matched.index.get_level_values(1)
                ).reset_index().drop_duplicates()

                self.matched = pd.merge(
                    self.matched.reset_index(),
                    left_cols,
                    on=left_index
                )

                suffixes = ('_LEFT', '_RIGHT')
                self.matched = pd.merge(
                    self.matched,
                    right_cols,
                    on=right_index,
                    suffixes=suffixes
                ).set_index([left_index, right_index])

                self.matched = self.matched.sort_values(['ENTITY_ID'])
                self.save_linked_data(self.matched, append)
                append = True
                self.matched = None
                # Remove grouped records from input dataset
                linked_data = self.get_rows_in(self.left_dataset, result.index)
                linked_data['ENTITY_ID'] = result['ENTITY_ID']
                self.linked = self.linked.append(linked_data)
                self.left_dataset = self.get_rows_not_in(self.left_dataset,
                                                         result.index)
                self.left_dataset.index.names = [self.left_index]

        linked_stats = self.total_linked.to_dict() \
            if self.total_linked is not None else {}

        for step in self.project['steps']:
            self.steps[step['seq']]['total_records_linked'] = \
                linked_stats.get(step['seq'], 0)

        # Delete temporary matched file.
        if os.path.isfile(match_file_path):
            os.remove(match_file_path)

        logger.info('Execution of de-duplication project {0} with Task id: {1} is completed.'
                    .format(self.project['name'], self.project['task_uuid']))

        logger.debug('<<--- run ---<<')

    def save(self):
        """
        Generates the de-duplicated file.
        :return:
        """
        logger.debug('>>--- save --->>')

        # Assign entity id to all remaining records.
        logger.info('Assigning entity id to all remaining records.')
        for rec_id in self.left_dataset.index.values:
            self.left_dataset.set_value(rec_id, 'ENTITY_ID',
                                        LinkBase.get_next_id())

        output = self.linked.append(self.left_dataset)
        output = output.sort_values(['ENTITY_ID'])

        dataset = self.project['datasets'][0]

        try:
            usecols = dataset['columns'] or self.left_columns
        except KeyError:
            usecols = self.left_columns

        self.left_dataset = pd.read_csv(dataset['url'],
                                        index_col=dataset['index_field'],
                                        usecols=usecols,
                                        skipinitialspace=True,
                                        dtype=self.left_dtypes)

        result = pd.concat([self.left_dataset, output['ENTITY_ID']], axis=1,
                           join='inner')
        cols = result.columns.tolist()
        cols.insert(0, cols.pop(cols.index('ENTITY_ID')))
        result = result[cols]

        self.total_entities = len(output.groupby(['ENTITY_ID']))

        logger.info('Total number of entities after de-duplication: {0}'.format(self.total_entities))
        # Storing deduplication result. It contains the original records plus the entity id of each record.
        deduped_file_path = self.project['output_root'] + link_config.get(
            'deduped_data_file', 'deduped_data.csv')

        result['ENTITY_ID'] = result['ENTITY_ID'].map(
            lambda x: '{:.0f}'.format(x)
            if pd.notnull(x)
            else np.nan)

        result.replace(np.nan, '', regex=True)
        result.to_csv(deduped_file_path, index_label=dataset['index_field'],
                      header=True, index=True)
        logger.info('De-duplicated file generated at {0}.'.format(deduped_file_path))

        logger.debug('<<--- save ---<<')
        return generate_linking_summary(self, self.project['output_root'])

from __future__ import absolute_import

import json

import numpy as np
import pandas as pd

from .algorithms import apply_encoding, apply_comparison
from .base import CHUNK_SIZE, MAX_PAIRS_SIZE, COLUMN_TYPES, LINKING_RELATIONSHIPS, _save_pairs
from cdilinker.reports.report import generate_linking_summary


class Step(object):
    def __init__(self, seq, left_data, link_method='DTR', output_root='./'):

        self.seq = seq
        self.left_data = left_data
        self.output_root = output_root
        self.right_data = None
        self.link_method = link_method or 'DTR'
        self.pairs = None
        self.linked = None
        self.matched_not_linked = None

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

    def get_pairs(self, left_fields, right_fields=None, transformations=None):

        if right_fields is None:
            right_fields = left_fields

        """
            TODO : Throw error if different number of left and right blocking variables are given.
            For each blocking variable there must be a corresponding  encoding method if encoding_method
            id not None.
        """

        suffixes = ('_L', '_R')

        # Remove records with null values for blocking variables.
        left_df = self.left_data.sort_index()
        left_df = left_df.dropna(axis=0, how='any', subset=np.unique(left_fields))
        # Create a copy of blocking columns to apply encoding methods
        left_on = [field + '_T' for field in left_fields]
        left_df[left_on] = left_df[left_fields]
        # Apply blocking variable encodings.
        for left, method in zip(left_on, transformations):
            left_df.loc[:, left] = apply_encoding(left_df[left], method)

        if (self.right_data is None):
            right_df = left_df
            right_on = left_on
            index_cols = {
                self.left_data.index.name + suffixes[0]: LinkBase.LEFT_INDEX,
                self.left_data.index.name + suffixes[1]: LinkBase.RIGHT_INDEX
            }
        else:
            right_df = self.right_data.sort_index()
            right_df = right_df.dropna(axis=0, how='any', subset=np.unique(right_fields))
            right_on = [field + '_T' for field in right_fields]
            right_df[right_on] = right_df[right_fields]
            # Apply blocking variable encodings.
            for right, method in zip(right_on, transformations):
                right_df[right] = apply_encoding(right_df[right], method)
            index_cols = {
                self.left_data.index.name: LinkBase.LEFT_INDEX,
                self.right_data.index.name: LinkBase.RIGHT_INDEX
            }

        left_chunks = int(np.ceil(len(left_df.index) / float(CHUNK_SIZE)))
        right_chunks = int(np.ceil(len(right_df.index) / float(CHUNK_SIZE)))

        pairs = None
        pairs_list = []
        append = False
        file_path = self.output_root + "pairs_temp.csv"
        total_pairs = 0
        for i in range(0, left_chunks):
            left_block = left_df.iloc[i * CHUNK_SIZE: (i + 1) * CHUNK_SIZE]
            for j in range(0, right_chunks):
                right_block = right_df.iloc[j * CHUNK_SIZE: (j + 1) * CHUNK_SIZE]

                if self.right_data is None and i > j: continue
                print "Finding record pairs for left block {0} and right block {1}".format(i, j)
                block_pairs = left_block.reset_index().merge(
                    right_block.reset_index(),
                    how='inner',
                    left_on=left_on,
                    right_on=right_on,
                    suffixes=suffixes
                )

                block_pairs.rename(columns=index_cols, inplace=True)
                if self.right_data is None:
                    block_pairs = block_pairs.loc[block_pairs[LinkBase.LEFT_INDEX] < block_pairs[LinkBase.RIGHT_INDEX]]
                block_pairs = block_pairs.set_index([LinkBase.LEFT_INDEX, LinkBase.RIGHT_INDEX])

                total_pairs += len(block_pairs.index)
                print "Total record pairs: {0}".format(total_pairs)
                if total_pairs > MAX_PAIRS_SIZE:
                    raise MemoryError(
                        'Record pairs size is too large. Please revise the blocking rules in step {0}.'.format(
                            self.seq))

                _save_pairs(file_path, block_pairs, append)
                append = True

    def _compare(self, left, right, compare_fn, **args):

        s1 = self.get_rows_in(self.left_data[left], self.pairs, level=0)

        if self.right_data is None:
            s2 = self.get_rows_in(self.left_data[right], self.pairs, level=1)
        else:
            s2 = self.get_rows_in(self.right_data[right], self.pairs, level=1)

        print "Compare Function : {0}".format(compare_fn)

        return apply_comparison(s1, s2, compare_fn, **args)

    def match(self, transforms, left_fields, right_fields=None):

        if right_fields is None:
            right_fields = left_fields

        append = False
        pairs_file_path = self.output_root + "pairs_temp.csv"
        match_file_path = self.output_root + "matched_temp.csv"
        for pairs in pd.read_csv(pairs_file_path, index_col=[LinkBase.LEFT_INDEX, LinkBase.RIGHT_INDEX],
                                 chunksize=CHUNK_SIZE):

            self.pairs = pairs.index
            if LinkBase.LEFT_ENTITY_ID in pairs.columns and LinkBase.RIGHT_ENTITY_ID in pairs.columns:
                paired = pairs[[LinkBase.LEFT_ENTITY_ID, LinkBase.RIGHT_ENTITY_ID]]
            else:
                paired = pd.DataFrame(index=self.pairs)

            if self.link_method == 'DTR':
                paired['matched'] = 1
                for left, right, fn in zip(left_fields, right_fields, transforms):
                    method = fn.get('name', 'EXACT')
                    args = fn.get('args') or {}
                    # args['method'] = self.link_method
                    print "Left : {0}, Right: {1}, Args: {2} ".format(left, right, fn)
                    result = self._compare(left, right, method, **args)
                    paired['matched'] &= result
            else:
                """
                TODO: Probabilistic linking
                """

            matched = paired.loc[lambda df: df.matched == 1, :]

            matched.drop('matched', axis=1, inplace=True)
            matched['STEP'] = self.seq

            _save_pairs(match_file_path, matched, append)
            append = True

        return matched

    def link(self):
        NotImplemented


class LinkStep(Step):
    def __init__(self, seq, left_data, right_data, link_method='DTR', output_root='./'):
        super(LinkStep, self).__init__(seq, left_data, link_method=link_method, output_root=output_root)
        self.right_data = right_data

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

        match_file_path = self.output_root + "matched_temp.csv"
        matched = pd.read_csv(match_file_path, index_col=[LinkBase.LEFT_INDEX, LinkBase.RIGHT_INDEX])

        group_field = LinkBase.RIGHT_ENTITY_ID
        filter_field = LinkBase.LEFT_ENTITY_ID
        if relationship == 'MT1':
            group_field, filter_field = filter_field, group_field

        reltionship_group = matched.groupby(group_field)
        linked = reltionship_group.filter(lambda x: len(x[filter_field].unique()) == 1)

        if relationship == '1T1':
            group_field, filter_field = filter_field, group_field

            reltionship_group = linked.groupby(group_field)
            linked = reltionship_group.filter(lambda x: len(x[filter_field].unique()) == 1)

        self.linked = linked
        self.linked['STEP'] = self.seq

        link_index = self.linked.reset_index()[[LinkBase.LEFT_ENTITY_ID, LinkBase.RIGHT_ENTITY_ID]].drop_duplicates()
        link_index = link_index.set_index([LinkBase.LEFT_ENTITY_ID, LinkBase.RIGHT_ENTITY_ID])
        link_index['LINK_ID'] = pd.Series([LinkBase.getNextId() for row in link_index.index], index=link_index.index)
        link_index['LINK_ID'] = link_index['LINK_ID'].map(
            lambda x: '{:.0f}'.format(x)
            if pd.notnull(x)
            else np.nan)
        self.linked = self.linked.join(link_index,
                                       on=[LinkBase.LEFT_ENTITY_ID, LinkBase.RIGHT_ENTITY_ID],
                                       how='inner')
        self.matched_not_linked = Step.get_rows_not_in(matched, self.linked.index)
        self.matched_not_linked['STEP'] = self.seq


class DeDupStep(Step):
    def __init__(self, seq, left_data, link_method='DTR', output_root='./'):
        super(DeDupStep, self).__init__(seq, left_data, link_method=link_method, output_root=output_root)

    def link(self, matched):
        left_index = matched.index.get_level_values(0).drop_duplicates()
        right_index = matched.index.get_level_values(1).drop_duplicates()
        link_index = left_index.union(right_index)
        link_index = link_index.rename('REC_ID')
        linked = pd.DataFrame(columns=['ENTITY_ID'], index=link_index)
        linked['ENTITY_ID'] = linked['ENTITY_ID'].map(
            lambda x: '{:.0f}'.format(x)
            if pd.notnull(x)
            else np.nan)
        linked['ENTITY_ID'].fillna(0, inplace=True)
        for left, right in matched.index.values:
            entity_id = linked.loc[left]['ENTITY_ID'] or \
                        linked.loc[right]['ENTITY_ID'] or \
                        LinkBase.getNextId()

            linked.set_value(left, 'ENTITY_ID', entity_id)
            linked.set_value(right, 'ENTITY_ID', entity_id)
            matched.set_value((left, right), 'ENTITY_ID', entity_id)

        return linked, matched


class LinkBase(object):
    LEFT_INDEX = 'LEFT_ID'
    RIGHT_INDEX = 'RIGHT_ID'

    LEFT_ENTITY_ID = 'LEFT_EID'
    RIGHT_ENTITY_ID = 'RIGHT_EID'

    BLOCK_SIZE = 1000000

    id = 0

    @classmethod
    def getNextId(cls):
        cls.id += 1
        return cls.id

    def __init__(self, project):
        self.project = project
        self.left_dataset = None
        self.right_dataset = None
        self.steps = None
        self.linked = None
        self.left_columns = self.right_columns = []
        self.total_records_linked = 0
        self.total_entities = 0
        self.total_linked = None

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

    def load(self):
        for step in self.project['steps']:
            self.left_columns = list(set(self.left_columns +
                                         step['blocking_schema'].get('left', []) +
                                         step['linking_schema'].get('left', [])))
            self.right_columns = list(set(self.right_columns +
                                          step['blocking_schema'].get('right', []) +
                                          step['linking_schema'].get('right', [])))

    def run(self):
        NotImplemented

    def save(self):
        NotImplemented


class Linker(LinkBase):
    def __init__(self, project):
        super(Linker, self).__init__(project)
        self.matched_not_linked = None
        self.left_index_type = "object"
        self.right_index_type = "object"

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

    def load(self):
        super(Linker, self).load()
        datasets = self.project['datasets']
        if datasets and len(datasets) > 1:


            self.left_columns += [datasets[0]['index_field'], datasets[0]['entity_field']]
            self.right_columns += [datasets[1]['index_field'], datasets[1]['entity_field']]

            if 'data_types' in datasets[0]:
                left_dtypes = {}
                for col_name, col_type in datasets[0]["data_types"].iteritems():
                    left_dtypes[col_name] = COLUMN_TYPES[col_type]
            else:
                left_dtypes = None

            if 'data_types' in datasets[1]:
                right_dtypes = {}
                for col_name, col_type in datasets[1]["data_types"].iteritems():
                    right_dtypes[col_name] = COLUMN_TYPES[col_type]
            else:
                right_dtypes = None

        self.left_index_type = left_dtypes[datasets[0]['index_field']]
        self.right_index_type = right_dtypes[datasets[1]['index_field']]

        try:
            left_usecols = datasets[0]['columns'] or self.left_columns
        except KeyError:
            left_usecols = self.left_columns

        self.left_dataset = pd.read_csv(datasets[0]['url'],
                                        index_col=datasets[0]['index_field'],
                                        usecols=left_usecols,
                                        skipinitialspace=True,
                                        dtype=left_dtypes)

        try:
            right_usecols = datasets[1]['columns'] or self.right_columns
        except KeyError:
            right_usecols = self.right_columns

        self.right_dataset = pd.read_csv(datasets[1]['url'],
                                         index_col=datasets[1]['index_field'],
                                         usecols=right_usecols,
                                         skipinitialspace=True,
                                         dtype=right_dtypes)

        self.left_dataset = self.left_dataset.set_index(self.left_dataset.index.rename(LinkBase.LEFT_INDEX))
        self.left_dataset.rename(columns={datasets[0]['entity_field']: LinkBase.LEFT_ENTITY_ID}, inplace=True)
        self.right_dataset = self.right_dataset.set_index(self.right_dataset.index.rename(LinkBase.RIGHT_INDEX))
        self.right_dataset.rename(columns={datasets[1]['entity_field']: LinkBase.RIGHT_ENTITY_ID}, inplace=True)

    def run(self):

        self.steps = {}
        self.total_records_linked = 0
        self.total_entities = 0
        print len(self.project['steps'])

        for step in self.project['steps']:
            self.steps[step['seq']] = {}
            print "Linking Step {0} :".format(step['seq'])
            link_step = LinkStep(step['seq'], self.left_dataset, self.right_dataset, 'DTR',
                                 output_root=self.project['output_root'])

            print "{0}.1) Finding record pairs satisfying blocking constraints...".format(step['seq'])
            link_step.get_pairs(
                left_fields=step['blocking_schema'].get('left'),
                right_fields=step['blocking_schema'].get('right'),
                transformations=step['blocking_schema'].get('transformations')
            )

            print "{0}.2) Applying linking constraints and finding matched records...".format(step['seq'])
            link_step.match(
                left_fields=step['linking_schema'].get('left'),
                right_fields=step['linking_schema'].get('right'),
                transforms=step['linking_schema'].get('comparisons')
            )

            print "{0}.3) Identifying the linked records based on the relationship type...".format(step['seq'])
            link_step.link(self.project['relationship_type'])

            self.steps[step['seq']]['total_records_linked'] = len(link_step.linked.index.values)
            self.total_records_linked += len(link_step.linked.index.values)
            self.steps[step['seq']]['total_entities'] = len(link_step.linked.groupby(['LEFT_EID', 'RIGHT_EID']))
            self.total_entities += self.steps[step['seq']]['total_entities']

            if not link_step.linked.empty:
                # Cretae EntityID - LinkId map
                left_links = link_step.linked[[LinkBase.LEFT_ENTITY_ID, 'LINK_ID']].drop_duplicates()
                left_links = left_links.reset_index().set_index(LinkBase.LEFT_ENTITY_ID)['LINK_ID']
                left_match = self.left_dataset.join(left_links, on=LinkBase.LEFT_ENTITY_ID, how='inner')

                linked = pd.merge(
                    left_match.reset_index(),
                    link_step.linked.reset_index(),
                    on=LinkBase.LEFT_INDEX,
                    how='left'
                )
                linked.drop('LEFT_EID_y', axis=1, inplace=True)

                linked.rename(columns={'LEFT_EID_x': 'LEFT_ENTITY_ID'}, inplace=True)

                right_links = link_step.linked[[LinkBase.RIGHT_ENTITY_ID, 'LINK_ID']].drop_duplicates()
                right_links = right_links.reset_index().set_index(LinkBase.RIGHT_ENTITY_ID)['LINK_ID']
                right_match = self.right_dataset.join(right_links, on=LinkBase.RIGHT_ENTITY_ID, how='inner')

                linked = pd.merge(
                    linked,
                    right_match.reset_index(),
                    on=LinkBase.RIGHT_INDEX,
                    how='right'
                )

                linked.drop('RIGHT_EID_x', axis=1, inplace=True)

                linked.drop(['LINK_ID_x', 'LINK_ID_y'], axis=1, inplace=True)

                linked.rename(columns={'RIGHT_EID_y': 'RIGHT_ENTITY_ID'}, inplace=True)
            else:
                linked = pd.DataFrame()

            self.linked = linked if self.linked is None else self.linked.append(linked)

            self.steps[step['seq']]['total_matched_not_linked'] = len(link_step.matched_not_linked.index.values)
            if self.matched_not_linked is None:
                self.matched_not_linked = link_step.matched_not_linked
            else:
                self.matched_not_linked = self.matched_not_linked.append(link_step.matched_not_linked)

            self.left_dataset = self.left_dataset[
                ~self.left_dataset[LinkBase.LEFT_ENTITY_ID].isin(link_step.linked[LinkBase.LEFT_ENTITY_ID])]
            self.right_dataset = self.right_dataset[
                ~self.right_dataset[LinkBase.RIGHT_ENTITY_ID].isin(link_step.linked[LinkBase.RIGHT_ENTITY_ID])]

            print "Number of records linked : {0}".format(len(self.linked))

    def save(self):
        grouped = self.matched_not_linked.reset_index().groupby(['LEFT_ID', 'RIGHT_ID', 'LEFT_EID', 'RIGHT_EID']).agg(
            {'STEP': 'min'})

        self.matched_not_linked = pd.DataFrame(grouped)

        print "Writing results to the output files ..."
        linked_file_path = self.project['output_root'] + "linked_data.csv"

        self.linked['STEP'] = self.linked['STEP'].map(
            lambda x: '{:.0f}'.format(x)
            if pd.notnull(x)
            else np.nan)

        self.linked['RIGHT_ENTITY_ID'] = self.linked['RIGHT_ENTITY_ID'].map(
            lambda x: '{:.0f}'.format(x)
            if pd.notnull(x)
            else np.nan)

        self.linked['LEFT_ENTITY_ID'] = self.linked['LEFT_ENTITY_ID'].map(
            lambda x: '{:.0f}'.format(x)
            if pd.notnull(x)
            else np.nan)

        if np.issubdtype(self.left_index_type, np.integer):
            self.linked[LinkBase.LEFT_INDEX] = self.linked[LinkBase.LEFT_INDEX].map(
                lambda x: '{:.0f}'.format(x)
                if pd.notnull(x)
                else np.nan)

        if np.issubdtype(self.right_index_type, np.integer):
            self.linked[LinkBase.RIGHT_INDEX] = self.linked[LinkBase.RIGHT_INDEX].map(
                lambda x: '{:.0f}'.format(x)
                if pd.notnull(x)
                else np.nan)


        self.linked = self.linked.sort_values(['LINK_ID'])
        self.linked.to_csv(linked_file_path, index=False)

        matched_file_path = self.project['output_root'] + "matched_not_linked_data.csv"
        self.matched_not_linked.to_csv(matched_file_path)

        return generate_linking_summary(self, self.project['output_root'])


class DeDeupProject(LinkBase):
    def __init__(self, name):
        super(DeDeupProject, self).__init__(name)
        self.matched = None
        self.out_filename = self.project['name'] + '_dedup_out.csv'
        self.deduped_filename = self.project['name'] + '_dedup_result.csv'

    def __str__(self):

        descriptor = super(DeDeupProject, self).__str__()

        data_dict = json.loads(descriptor)
        dataset = self.project['datasets'][0]
        data_dict['dataset'] = dataset['name']

        return json.dumps(data_dict, indent=4)

    def _save_linked_data(self, data, append=False):

        file_path = self.project['output_root'] + self.out_filename
        if not append:
            data.to_csv(file_path)
        else:
            with open(file_path, 'a') as f:
                data.to_csv(f, header=False)

    def load(self):
        super(DeDeupProject, self).load()
        if self.project['datasets'] and len(self.project['datasets']) > 0:
            dataset = self.project['datasets'][0]
            self.left_columns.append(dataset['index_field'])

            if 'data_types' in dataset:
                left_dtypes = {}
                for col_name, col_type in dataset["data_types"].iteritems():
                    left_dtypes[col_name] = COLUMN_TYPES[col_type]
            else:
                left_dtypes = None

            print "Dataset header {}".format(self.left_columns)

            try:
                usecols = dataset['columns'] or self.left_columns
            except KeyError:
                usecols = self.left_columns

            self.left_dataset = pd.read_csv(dataset['url'],
                                            index_col=dataset['index_field'],
                                            usecols=usecols,
                                            skipinitialspace=True,
                                            dtype=left_dtypes)

            self.left_dataset = self.left_dataset.set_index(self.left_dataset.index.rename(LinkBase.LEFT_INDEX))

    def run(self):

        append = False

        self.steps = {}
        self.linked = pd.DataFrame()
        total_step_entities = None
        for step in self.project['steps']:
            self.steps[step['seq']] = {}
            print "De-duplication Step {0} :".format(step['seq'])
            dedup_step = DeDupStep(step['seq'], self.left_dataset, link_method='DTR',
                                   output_root=self.project['output_root'])

            print "{0}.1) Finding record pairs satisfying blocking constraints...".format(step['seq'])
            dedup_step.get_pairs(
                left_fields=step['blocking_schema'].get('left'),
                transformations=step['blocking_schema'].get('transformations')
            )

            print "{0}.2) Applying linking constraints and finding matched records...".format(step['seq'])
            dedup_step.match(
                left_fields=step['linking_schema'].get('left'),
                right_fields=step['linking_schema'].get('left'),
                transforms=step['linking_schema'].get('comparisons')
            )

            match_file_path = self.project['output_root'] + "matched_temp.csv"
            matched = pd.read_csv(match_file_path, index_col=[LinkBase.LEFT_INDEX, LinkBase.RIGHT_INDEX])
            self.matched = matched if self.matched is None else self.matched.append(matched)
            self.matched = self.matched.groupby(level=[0, 1]).min()
            self.matched = pd.DataFrame(self.matched)
            if step['group'] and not self.matched.empty:
                self.total_records_linked += len(self.matched.index)
                # Group rows that blong to the same entity and assign entity id
                result, self.matched = dedup_step.link(self.matched)
                step_group = self.matched.groupby(['STEP'])

                total_linked = step_group.size()

                self.total_linked = total_linked if self.total_linked is None else self.total_linked.append(
                    total_linked)

                left_cols = Step.get_rows_in(
                    self.left_dataset,
                    self.matched.index.get_level_values(0)
                ).reset_index().drop_duplicates()

                right_cols = Step.get_rows_in(
                    self.left_dataset,
                    self.matched.index.get_level_values(1)
                ).reset_index().drop_duplicates()

                self.matched = pd.merge(
                    self.matched.reset_index(),
                    left_cols,
                    on=LinkBase.LEFT_INDEX
                )

                suffixes = ('_LEFT', '_RIGHT')
                self.matched = pd.merge(
                    self.matched,
                    right_cols,
                    on=LinkBase.RIGHT_INDEX,
                    suffixes=suffixes
                ).set_index([LinkBase.LEFT_INDEX, LinkBase.RIGHT_INDEX])

                self.matched = self.matched.sort_values(['ENTITY_ID'])
                self._save_linked_data(self.matched, append)
                append = True
                self.matched = None
                # Remove grouped records from input dataset
                linked_data = Step.get_rows_in(self.left_dataset, result.index)
                linked_data['ENTITY_ID'] = result['ENTITY_ID']
                self.linked = self.linked.append(linked_data)
                self.left_dataset = Step.get_rows_not_in(self.left_dataset, result.index)
                self.left_dataset = self.left_dataset.set_index(self.left_dataset.index.rename(LinkBase.LEFT_INDEX))

        linked_stats = self.total_linked.to_dict() if self.total_linked is not None else {}

        for step in self.project['steps']:
            self.steps[step['seq']]['total_records_linked'] = linked_stats.get(step['seq'], 0)

    def save(self):
        """
        Generates the de-duplicated file.
        :return:
        """

        # Assign entity id to all remaining records.
        for rec_id in self.left_dataset.index.values:
            self.left_dataset.set_value(rec_id, 'ENTITY_ID', LinkBase.getNextId())

        output = self.linked.append(self.left_dataset)
        output = output.sort(['ENTITY_ID'])

        dataset = self.project['datasets'][0]

        try:
            usecols = dataset['columns'] or self.left_columns
        except KeyError:
            usecols = self.left_columns

        self.left_dataset = pd.read_csv(dataset['url'],
                                        index_col=dataset['index_field'],
                                        usecols=usecols,
                                        skipinitialspace=True)

        result = pd.concat([self.left_dataset, output['ENTITY_ID']], axis=1, join='inner')

        self.total_entities = len(output.groupby(['ENTITY_ID']))
        file_path = self.project['output_root'] + self.deduped_filename

        result['ENTITY_ID'] = result['ENTITY_ID'].map(
            lambda x: '{:.0f}'.format(x)
            if pd.notnull(x)
            else np.nan)

        result.to_csv(file_path, index_label=dataset['index_field'], header=True, index=True)

        return generate_linking_summary(self, self.project['output_root'])


def run_project(project):
    '''
    Runs a De-Duplication or Linking project, depending on the project type.
    :param project: Linking/De-duplication project
    :return: Project results summary
    '''
    if project['type'] == 'DEDUP':
        task = DeDeupProject(project)
    else:
        task = Linker(project)

    if task:
        task.load()
        task.run()
        return task.save()

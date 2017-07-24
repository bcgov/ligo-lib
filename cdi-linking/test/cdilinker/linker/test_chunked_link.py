import os
import pytest
import shutil

from cdilinker.linker.chunked_link import ChunkedLink
from cdilinker.linker.files import LinkFiles
from test.cdilinker.utils import Utils


class TestChunkedLink(object):
    @staticmethod
    @pytest.fixture(scope="class")
    def project():
        """Read test_jtst_educ_linking project configuration"""
        return Utils.load_project_data('test_jtst_educ_linking.json')

    @staticmethod
    @pytest.fixture
    def linker(project):
        if not os.path.exists(project['temp_path']):
            os.makedirs(project['temp_path'])
        yield ChunkedLink(project)

        # Teardown and clean up
        if os.path.isfile(project['temp_path'] +
                          LinkFiles.TEMP_MATCHED_FILE):
            os.remove(project['temp_path'] + LinkFiles.TEMP_MATCHED_FILE)
        if os.path.isfile(project['output_root'] + 'left_file.csv'):
            os.remove(project['output_root'] + 'left_file.csv')
        if os.path.isfile(project['output_root'] + 'right_file.csv'):
            os.remove(project['output_root'] + 'right_file.csv')
        if os.path.isfile(project['temp_path'] +
                          LinkFiles.TEMP_STEP_LINKED_FILE):
            os.remove(project['temp_path'] + LinkFiles.TEMP_STEP_LINKED_FILE)
        if os.path.isfile(project['temp_path'] +
                          LinkFiles.TEMP_LINKED_RECORDS):
            os.remove(project['temp_path'] + LinkFiles.TEMP_LINKED_RECORDS)
        if os.path.isfile(project['temp_path'] +
                          LinkFiles.MATCHED_RECORDS):
            os.remove(project['temp_path'] + LinkFiles.MATCHED_RECORDS)
        if os.path.isfile(project['output_root'] +
                          'matched_not_linked_data.csv'):
            os.remove(project['output_root'] + 'matched_not_linked_data.csv')
        if os.path.isfile(project['output_root'] + 'linked_data.csv'):
            os.remove(project['output_root'] + 'linked_data.csv')
        if os.path.isfile(project['output_root'] + project['name'] +
                          '_summary.pdf'):
            os.remove(project['output_root'] + project['name'] +
                      '_summary.pdf')
        if os.path.exists(project['temp_path']):
            shutil.rmtree(project['temp_path'])

    def test_init_none(self):
        """Ensure initialization does not proceed with empty JSON"""
        with pytest.raises(TypeError):
            ChunkedLink(None)

    def test_init(self, project, linker):
        """Ensure initialization sets fields correctly"""
        assert linker.matched_not_linked is None
        assert linker.project_type == 'LINK'
        assert linker.left_index == project['datasets'][0]['index_field']
        assert linker.right_index == project['datasets'][1]['index_field']
        assert linker.left_entity == project['datasets'][0]['entity_field']
        assert linker.right_entity == project['datasets'][1]['entity_field']

    def test_str(self, linker):
        """Should not be throwing a JSONDecodeError"""
        import json

        json.loads(str(linker))

    def test_load_data(self, project, linker):
        """Tests if the data is properly loaded"""
        linker.load_data()

        assert linker.left_columns is not None
        assert len(linker.left_columns) == \
            len(project['datasets'][0]['columns'])
        assert linker.left_dtypes is not None
        assert len(linker.left_dtypes) == 6
        assert linker.right_columns is not None
        assert len(linker.right_columns) == \
            len(project['datasets'][1]['columns'])
        assert linker.right_dtypes is not None
        assert len(linker.right_dtypes) == 6
        assert linker.left_index is not None
        assert linker.left_index == project['datasets'][0]['index_field']
        assert linker.right_index is not None
        assert linker.right_index == project['datasets'][1]['index_field']
        assert os.path.isfile(project['output_root'] + 'left_file.csv')
        assert os.path.isfile(project['output_root'] + 'right_file.csv')

    def test_groupby_unique_filter(self, project, linker):
        """Checks unique grouping is behaving correctly"""
        step = project['steps'][0]
        group_field = 'RIGHT_' + project['datasets'][1]['entity_field']
        filter_field = 'LEFT_' + project['datasets'][0]['entity_field']
        matched_not_linked_filename = project['output_root'] + \
            'matched_not_linked_data.csv'
        matched_file = project['temp_path'] + LinkFiles.MATCHED_RECORDS
        open(matched_file, 'w').close()

        linker.load_data()
        linker.pair_n_match(step=step['seq'],
                            link_method=step['linking_method'],
                            blocking=step['blocking_schema'],
                            linking=step['linking_schema'],
                            matched_file=matched_file)
        temp_filename, stats = \
            linker.groupby_unique_filter(matched_file,
                                         group_col=group_field,
                                         filter_col=filter_field,
                                         not_linked_filename=matched_not_linked_filename,
                                         add_link_id=False,
                                         linked_filename=None)

        assert temp_filename is not None
        assert temp_filename == \
            project['temp_path'] + LinkFiles.TEMP_LINK_FILTERED
        assert stats is not None
        assert len(stats) == 3
        assert stats['total_linked'] == 0
        assert stats['total_filtered'] == 0
        assert stats['total_records_linked'] == 72

    def test_link(self, project, linker):
        """Tests link and filter functionality"""
        step = project['steps'][0]
        matched_file = project['temp_path'] + LinkFiles.MATCHED_RECORDS
        open(matched_file, 'w').close()

        linker.load_data()
        linker.pair_n_match(step=step['seq'],
                            link_method=step['linking_method'],
                            blocking=step['blocking_schema'],
                            linking=step['linking_schema'],
                            matched_file=matched_file)
        stats = linker.link(project['relationship_type'])

        assert stats is not None
        assert len(stats) == 3
        assert stats['total_linked'] == 15
        assert stats['total_filtered'] == 0
        assert stats['total_records_linked'] == 72

    def test_extract_linked_records(self, project, linker):
        """Tests if linked records are removed"""
        step = project['steps'][0]
        step_linked = project['temp_path'] + LinkFiles.TEMP_STEP_LINKED_FILE
        data_filename = project['output_root'] + 'left_file.csv'
        matched_file = project['temp_path'] + LinkFiles.MATCHED_RECORDS
        open(matched_file, 'w').close()

        linker.load_data()
        linker.pair_n_match(step=step['seq'],
                            link_method=step['linking_method'],
                            blocking=step['blocking_schema'],
                            linking=step['linking_schema'],
                            matched_file=matched_file)
        linker.link(project['relationship_type'])

        assert os.path.isfile(step_linked)
        linker.extract_linked_records(linked_filename=step_linked, prefix='LEFT_')

        assert os.path.isfile(data_filename)
        assert Utils.file_len(data_filename) == 929
        assert os.path.isfile(step_linked)
        assert Utils.file_len(step_linked) == 73

    def test_run(self, project, linker):
        """Tests if the task can be run"""
        linker.load_data()
        linker.run()

        assert linker.steps is not None
        assert len(linker.steps) == len(project['steps'])
        assert linker.total_entities is not None
        assert linker.total_entities == 15
        assert linker.total_records_linked is not None
        assert linker.total_records_linked == 72
        assert os.path.isfile(project['temp_path'] +
                              LinkFiles.TEMP_LINKED_RECORDS)
        assert not os.path.isfile(project['temp_path'] +
                                  LinkFiles.TEMP_MATCHED_FILE)
        assert not os.path.isfile(project['temp_path'] +
                                  LinkFiles.MATCHED_RECORDS)

    def test_save(self, project, linker):
        """Tests if the execution results are saved"""
        linker.load_data()
        linker.run()
        linker.save()

        assert linker.total_entities is not None
        assert linker.total_entities == 15
        assert not os.path.isfile(project['temp_path'] +
                                  LinkFiles.TEMP_LINKED_RECORDS)
        assert not os.path.isfile(project['temp_path'] + 'left_file.csv')
        assert not os.path.isfile(project['temp_path'] + 'right_file.csv')
        assert os.path.isfile(project['output_root'] +
                              'matched_not_linked_data.csv')
        assert os.path.isfile(project['output_root'] + 'linked_data.csv')
        assert os.path.isfile(project['output_root'] + project['name'] +
                              '_summary.pdf')

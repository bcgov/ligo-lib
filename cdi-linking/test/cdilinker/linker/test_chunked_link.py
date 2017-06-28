import json
import os
import pytest

from cdilinker.linker.files import LinkFiles
from cdilinker.linker.chunked_link import Linker


class TestLinkerChunkedLink(object):
    @pytest.fixture(scope="class")
    def project(self):
        """Read test_jtst_educ_linking project configuration"""
        import pandas as pd
        import uuid

        # Suppress SettingWithCopyWarning warnings from Pandas
        # https://stackoverflow.com/q/20625582
        pd.options.mode.chained_assignment = None  # default='warn'

        with open(os.path.join(os.path.dirname(__file__), '..', 'data',
                               'test_jtst_educ_linking.json')) as data_file:
            project = json.load(data_file)

        # Add task_uuid to this project
        project['task_uuid'] = uuid.uuid4().hex
        yield project

        # Teardown and clean up
        if os.path.isfile(project['output_root'] +
                          LinkFiles.TEMP_MATCHED_FILE):
            os.remove(project['output_root'] + LinkFiles.TEMP_MATCHED_FILE)
        if os.path.isfile(project['output_root'] + 'left_file.csv'):
            os.remove(project['output_root'] + 'left_file.csv')
        if os.path.isfile(project['output_root'] + 'right_file.csv'):
            os.remove(project['output_root'] + 'right_file.csv')
        if os.path.isfile(project['output_root'] +
                          LinkFiles.TEMP_LINKED_RECORDS):
            os.remove(project['output_root'] + LinkFiles.TEMP_LINKED_RECORDS)
        if os.path.isfile(project['output_root'] +
                          LinkFiles.MATCHED_RECORDS):
            os.remove(project['output_root'] + LinkFiles.MATCHED_RECORDS)
        if os.path.isfile(project['output_root'] +
                          'matched_not_linked_data.csv'):
            os.remove(project['output_root'] + 'matched_not_linked_data.csv')
        if os.path.isfile(project['output_root'] + 'linked_data.csv'):
            os.remove(project['output_root'] + 'linked_data.csv')
        if os.path.isfile(project['output_root'] + project['name'] +
                          '_summary.pdf'):
            os.remove(project['output_root'] + project['name'] +
                      '_summary.pdf')

    def test_init_none(self):
        """Ensure initialization does not proceed with empty JSON"""
        with pytest.raises(TypeError):
            Linker(None)

    def test_init(self, project):
        """Ensure initialization sets fields correctly"""
        linker = Linker(project)

        assert linker.matched_not_linked is None
        assert linker.project_type == 'LINK'
        assert linker.left_index == project['datasets'][0]['index_field']
        assert linker.right_index == project['datasets'][1]['index_field']
        assert linker.left_entity == project['datasets'][0]['entity_field']
        assert linker.right_entity == project['datasets'][1]['entity_field']

    def test_str(self, project):
        """Should not be throwing a JSONDecodeError"""
        json.loads(str(Linker(project)))

    def test_load_data(self, project):
        """Tests if the data is properly loaded"""
        linker = Linker(project)
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

    def test_groupby_unique_filter(self, project):
        """Checks unique grouping is behaving correctly"""
        NotImplemented

    def test_link(self, project):
        """Tests link and filter functionality"""
        NotImplemented

    def test_extract_linked_records(self, project):
        """Tests if linked records are removed"""
        NotImplemented

    def test_run(self, project):
        """Tests if the task can be run"""
        linker = Linker(project)
        linker.load_data()
        linker.run()

        assert linker.steps is not None
        assert len(linker.steps) == len(project['steps'])
        assert linker.total_entities is not None
        assert linker.total_entities == 15
        assert linker.total_records_linked is not None
        assert linker.total_records_linked == 72
        assert linker.linked is None
        assert os.path.isfile(project['output_root'] +
                              LinkFiles.TEMP_LINKED_RECORDS)
        assert not os.path.isfile(project['output_root'] +
                                  LinkFiles.TEMP_MATCHED_FILE)
        assert not os.path.isfile(project['output_root'] +
                                  LinkFiles.MATCHED_RECORDS)


    def test_save(self, project):
        """Tests if the execution results are saved"""
        linker = Linker(project)
        linker.load_data()
        linker.run()
        linker.save()

        assert linker.total_entities is not None
        assert linker.total_entities == 15
        assert os.path.isfile(project['output_root'] +
                              'matched_not_linked_data.csv')
        assert os.path.isfile(project['output_root'] + 'linked_data.csv')
        assert os.path.isfile(project['output_root'] + project['name'] +
                              '_summary.pdf')

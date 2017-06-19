import json
import os
import pytest

from cdilinker.linker.files import LinkFiles
from cdilinker.linker.link import Linker


class TestLinkerLink(object):
    @pytest.fixture(scope="class")
    def project(self):
        """Read test_jtst_educ_linking project configuration"""
        __location__ = os.path.realpath(
            os.path.join(os.getcwd(), os.path.dirname(__file__)))
        with open(os.path.join(
                __location__, "data/test_jtst_educ_linking.json")) \
                as data_file:
            project = json.load(data_file)
        yield project

        # Teardown and clean up

    def test_init_none(self):
        """Ensure initialization does not proceed with empty JSON"""
        with pytest.raises(TypeError):
            Linker(None)

    def test_init(self, project):
        """Ensure initialization sets fields correctly"""
        linker = Linker(project)

        assert linker.matched_not_linked is None
        assert linker.left_index_type == "object"
        assert linker.right_index_type == "object"
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
        assert linker.right_columns is not None
        assert len(linker.right_columns) == \
            len(project['datasets'][1]['columns'])
        assert linker.left_index is not None
        assert linker.left_index == project['datasets'][0]['index_field']
        assert linker.right_index is not None
        assert linker.right_index == project['datasets'][1]['index_field']
        assert linker.left_dataset is not None
        assert len(linker.left_dataset) == 999
        assert linker.right_dataset is not None
        assert len(linker.right_dataset) == 999

    def test_link(self, project):
        """Tests link and filter functionality"""
        NotImplemented

    def test_run(self, project):
        """Tests if the task can be run"""
        linker = Linker(project)
        linker.load_data()
        linker.run()

        assert not os.path.isfile(project['output_root'] +
                                  LinkFiles.TEMP_MATCHED_FILE)
        assert linker.steps is not None
        assert len(linker.steps) == len(project['steps'])
        assert linker.total_records_linked == 144
        assert linker.total_entities == 30

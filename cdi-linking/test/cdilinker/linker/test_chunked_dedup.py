import json
import os
import pytest

from cdilinker.linker.files import LinkFiles
from cdilinker.linker.chunked_dedup import DeDeupProject


class TestLinkerDedup(object):
    @pytest.fixture(scope="class")
    def project(self):
        """Read test_jtst_dedup project configuration"""
        import uuid

        with open(os.path.join(os.path.dirname(__file__), '..', 'data',
                               'test_jtst_dedup.json')) as data_file:
            project = json.load(data_file)

        # Add task_uuid to this project
        project['task_uuid'] = uuid.uuid4().hex
        yield project

        # Teardown and clean up
        if os.path.isfile(project['output_root'] + 'left_file.csv'):
            os.remove(project['output_root'] + 'left_file.csv')
        if os.path.isfile(project['output_root'] + 'matched_records.csv'):
            os.remove(project['output_root'] + 'matched_records.csv')
        if os.path.isfile(project['output_root'] + 'dedup_matched.csv'):
            os.remove(project['output_root'] + 'dedup_matched.csv')
        if os.path.isfile(project['output_root'] + 'deduped_data.csv'):
            os.remove(project['output_root'] + 'deduped_data.csv')
        if os.path.isfile(project['output_root'] + project['name'] +
                          '_summary.pdf'):
            os.remove(project['output_root'] + project['name'] +
                      '_summary.pdf')

    def test_init_none(self):
        """Ensure initialization does not proceed with empty JSON"""
        with pytest.raises(TypeError):
            DeDeupProject(None)

    def test_init(self, project):
        """Ensure initialization sets fields correctly"""
        ddp = DeDeupProject(project)

        assert ddp.project_type == 'DEDUP'
        assert ddp.left_index == project['datasets'][0]['index_field']
        assert ddp.right_index == project['datasets'][0]['index_field']
        assert ddp.matched is None
        assert ddp.left_dtypes is None
        assert ddp.right_dtypes is None

    def test_str(self, project):
        """Should not be throwing a JSONDecodeError"""
        json.loads(str(DeDeupProject(project)))

    def test_load_data(self, project):
        """Tests if the data is properly loaded"""
        ddp = DeDeupProject(project)
        ddp.load_data()

        assert ddp.left_columns is not None
        assert len(ddp.left_columns) == 7
        assert ddp.left_dtypes is not None
        assert len(ddp.left_dtypes) == 7
        assert ddp.right_columns is not None
        assert len(ddp.right_columns) == 7
        assert ddp.right_dtypes is not None
        assert len(ddp.right_dtypes) == 7

    def test_link_pairs(self, project):
        """Tests if link_pairs behaves as intended"""
        NotImplemented

    def test_extract_rows(self, project):
        """Tests if linked records are removed from input data"""
        NotImplemented

    def test_run(self, project):
        """Tests if the task can be run"""
        ddp = DeDeupProject(project)
        ddp.load_data()
        ddp.run()

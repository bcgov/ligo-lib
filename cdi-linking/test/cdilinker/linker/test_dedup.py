import os
import pytest

from cdilinker.linker.dedup import DeDeupProject
from cdilinker.linker.files import LinkFiles
from test.cdilinker.utils import Utils


class TestDedup(object):
    @pytest.fixture(scope="class")
    def project(self):
        """Read test_jtst_dedup project configuration"""
        return Utils.load_project('test_jtst_dedup.json')

    @pytest.fixture
    def ddp(self, project):
        yield DeDeupProject(project)

        # Teardown and clean up
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

    def test_init(self, project, ddp):
        """Ensure initialization sets fields correctly"""
        assert ddp.project_type == 'DEDUP'
        assert ddp.left_index == project['datasets'][0]['index_field']
        assert ddp.right_index == project['datasets'][0]['index_field']
        assert ddp.matched is None
        assert ddp.left_dtypes is None
        assert ddp.right_dtypes is None

    def test_str(self, ddp):
        """Should not be throwing a JSONDecodeError"""
        import json

        json.loads(str(ddp))

    def test_save_linked_data(self, project, ddp):
        """Tests if the deduped matched file exists"""
        import pandas as pd

        ddp.save_linked_data(pd.DataFrame())

        assert os.path.isfile(project['output_root'] + 'dedup_matched.csv')

    def test_load_data(self, ddp):
        """Tests if the data is properly loaded"""
        ddp.load_data()

        assert ddp.left_columns is not None
        assert len(ddp.left_columns) == 6
        assert ddp.left_dtypes is not None
        assert len(ddp.left_dtypes) == 7
        assert ddp.left_dataset is not None
        assert len(ddp.left_dataset) == 999

    def test_run(self, project, ddp):
        """Tests if the task can be run"""
        ddp.load_data()
        ddp.run()

        assert ddp.steps is not None
        assert len(ddp.steps) == len(project['steps'])
        assert ddp.linked is not None
        assert len(ddp.linked) == 2
        assert ddp.matched is None
        assert ddp.total_linked is not None
        assert len(ddp.total_linked) == 1
        assert not os.path.isfile(project['output_root'] +
                                  LinkFiles.TEMP_MATCHED_FILE)

    def test_save(self, project, ddp):
        """Tests if the execution results are saved"""
        ddp.load_data()
        ddp.run()
        ddp.save()

        assert ddp.total_entities is not None
        assert ddp.total_entities == 998
        assert os.path.isfile(project['output_root'] + 'deduped_data.csv')
        assert os.path.isfile(project['output_root'] + project['name'] +
                              '_summary.pdf')

import json
import os
import pandas as pd
import pytest

from cdilinker.linker.files import LinkFiles
from cdilinker.linker.dedup import DeDeupProject


class TestLinkerDedup(object):
    @pytest.fixture(scope="class")
    def project(self):
        """Read test_jtst_dedup project configuration"""
        __location__ = os.path.realpath(
            os.path.join(os.getcwd(), os.path.dirname(__file__)))
        with open(os.path.join(__location__, "data/test_jtst_dedup.json")) \
                as data_file:
            project = json.load(data_file)
        yield project

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

    def test_save_linked_data(self, project):
        """Tests if the deduped matched file exists"""
        # TODO Add dynamic file directory checking and removal
        ddp = DeDeupProject(project)
        df = pd.DataFrame()
        ddp._save_linked_data(df)

        assert os.path.isfile(project['output_root'] + 'dedup_matched.csv')

    def test_load_data(self, project):
        """Tests if the data is properly loaded"""
        ddp = DeDeupProject(project)
        ddp.load_data()

        assert ddp.left_columns is not None
        assert len(ddp.left_columns) == 6
        assert ddp.left_dtypes is not None
        assert ddp.left_dataset is not None

    def test_run(self, project):
        """Tests if the task can be run"""
        ddp = DeDeupProject(project)
        ddp.load_data()
        ddp.run()

        assert not os.path.isfile(project['output_root'] +
                                  LinkFiles.TEMP_MATCHED_FILE)
        assert ddp.steps is not None
        assert len(ddp.steps) == len(project['steps'])
        assert ddp.linked is not None
        assert ddp.matched is None
        assert ddp.total_linked is None

    def test_save(self, project):
        """Tests if the execution results are saved"""
        ddp = DeDeupProject(project)
        ddp.load_data()
        ddp.run()
        ddp.save()

        assert ddp.total_entities is not None
        assert ddp.total_entities == 98
        assert os.path.isfile(project['output_root'] + 'deduped_data.csv')
        assert os.path.isfile(project['output_root'] + project['name'] +
                              '_summary.pdf')

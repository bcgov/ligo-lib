import json
import os
import pytest

from cdilinker.linker.files import LinkFiles
from cdilinker.linker.chunked_dedup import DeDeupProject


class TestLinkerChunkedDedup(object):
    @pytest.fixture(scope="class")
    def project(self):
        """Read test_jtst_dedup project configuration"""
        import pandas as pd
        import uuid

        # Suppress SettingWithCopyWarning warnings from Pandas
        # https://stackoverflow.com/q/20625582
        pd.options.mode.chained_assignment = None  # default='warn'

        with open(os.path.join(os.path.dirname(__file__), '..', 'data',
                               'test_jtst_dedup.json')) as data_file:
            project = json.load(data_file)

        # Add task_uuid to this project
        project['task_uuid'] = uuid.uuid4().hex
        yield project

        # Teardown and clean up
        if os.path.isfile(project['output_root'] + 'left_file.csv'):
            os.remove(project['output_root'] + 'left_file.csv')
        if os.path.isfile(project['output_root'] +
                          LinkFiles.TEMP_DEDUP_ALL_SELECTED):
            os.remove(project['output_root'] +
                      LinkFiles.TEMP_DEDUP_ALL_SELECTED)
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
        step = project['steps'][1]
        matched_file = project['output_root'] + LinkFiles.MATCHED_RECORDS
        open(matched_file, 'w').close()

        ddp = DeDeupProject(project)
        ddp.load_data()
        ddp.pair_n_match(step=step['seq'],
                         link_method=step['linking_method'],
                         blocking=step['blocking_schema'],
                         linking=step['linking_schema'],
                         matched_file=matched_file)
        value = ddp.link_pairs()

        assert value is not None
        assert value == 0

    def test_extract_rows(self, project):
        """Tests if linked records are removed from input data"""
        step = project['steps'][1]
        matched_file = project['output_root'] + LinkFiles.MATCHED_RECORDS
        open(matched_file, 'w').close()
        linked_file = project['output_root'] + LinkFiles.TEMP_ENTITIES_FILE

        ddp = DeDeupProject(project)
        ddp.load_data()
        ddp.pair_n_match(step=step['seq'],
                         link_method=step['linking_method'],
                         blocking=step['blocking_schema'],
                         linking=step['linking_schema'],
                         matched_file=matched_file)
        ddp.link_pairs()
        ddp.extract_rows(data_filename=ddp.left_file,
                         data_id=ddp.left_index,
                         index_filename=linked_file, index_id='REC_ID',
                         index_cols=['ENTITY_ID'])

        assert not os.path.isfile(project['output_root'] +
                                  LinkFiles.TEMP_STEP_REMAINED)
        assert os.path.isfile(ddp.left_file)
        with open(ddp.left_file) as f:
            for left_file_size, l in enumerate(f):
                pass
        assert left_file_size == 999

    def test_run(self, project):
        """Tests if the task can be run"""
        ddp = DeDeupProject(project)
        ddp.load_data()
        ddp.run()

        assert ddp.steps is not None
        assert len(ddp.steps) == 2
        assert ddp.linked is not None
        assert len(ddp.linked) == 0
        assert ddp.total_entities is not None
        assert ddp.total_entities == 1
        assert ddp.total_records_linked is not None
        assert ddp.total_records_linked == 1

        assert not os.path.isfile(project['output_root'] +
                                  LinkFiles.MATCHED_RECORDS)
        assert not os.path.isfile(project['output_root'] +
                                  LinkFiles.TEMP_ENTITIES_FILE)
        assert not os.path.isfile(project['output_root'] +
                                  LinkFiles.TEMP_DEDUP_STEP_SELECTED)
        assert os.path.isfile(project['output_root'] +
                              LinkFiles.TEMP_DEDUP_ALL_SELECTED)

    def test_save(self, project):
        """Tests if the execution results are saved"""
        ddp = DeDeupProject(project)
        ddp.load_data()
        ddp.run()
        ddp.save()

        assert ddp.total_entities is not None
        assert ddp.total_entities == 998
        assert os.path.isfile(project['output_root'] + 'deduped_data.csv')
        assert os.path.isfile(project['output_root'] + project['name'] +
                              '_summary.pdf')

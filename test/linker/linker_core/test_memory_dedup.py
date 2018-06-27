import os
import pytest
import shutil

from linker.linker_core.memory_dedup import MemoryDedup
from linker.linker_core.files import LinkFiles
from test.linker.utils import Utils


@pytest.fixture(scope="module")
def project():
    """Read test_jtst_dedup project configuration"""
    return Utils.load_project_data('test_jtst_dedup.json')


@pytest.fixture
def ddp(request, project):
    if not os.path.exists(project['temp_path']):
        os.makedirs(project['temp_path'])

    def teardown():
        if os.path.isfile(project['output_root'] + 'dedup_matched.csv'):
            os.remove(project['output_root'] + 'dedup_matched.csv')
        if os.path.isfile(project['output_root'] + 'deduped_data.csv'):
            os.remove(project['output_root'] + 'deduped_data.csv')
        if os.path.isfile(project['output_root'] + project['name'] +
                          '_summary.pdf'):
            os.remove(project['output_root'] + project['name'] +
                      '_summary.pdf')
        if os.path.exists(project['temp_path']):
            shutil.rmtree(project['temp_path'])

    request.addfinalizer(teardown)
    return MemoryDedup(project)


def test_init_none():
    """Ensure initialization does not proceed with empty JSON"""
    with pytest.raises(TypeError):
        MemoryDedup(None)


def test_init(project, ddp):
    """Ensure initialization sets fields correctly"""
    assert ddp.project_type == 'DEDUP'
    assert ddp.left_index == project['datasets'][0]['index_field']
    assert ddp.right_index == project['datasets'][0]['index_field']
    assert ddp.matched is None
    assert ddp.left_dtypes is None
    assert ddp.right_dtypes is None


def test_str(ddp):
    """Should not be throwing a JSONDecodeError"""
    import json

    json.loads(str(ddp))


def test_save_linked_data(project, ddp):
    """Tests if the deduped matched file exists"""
    import pandas as pd

    ddp.save_linked_data(pd.DataFrame())

    assert os.path.isfile(project['output_root'] + 'dedup_matched.csv')


def test_load_data(ddp):
    """Tests if the data is properly loaded"""
    ddp.load_data()

    assert ddp.left_columns is not None
    assert len(ddp.left_columns) == 6
    assert ddp.left_dtypes is not None
    assert len(ddp.left_dtypes) == 7
    assert ddp.left_dataset is not None
    assert len(ddp.left_dataset) == 999


def test_run(project, ddp):
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
    assert not os.path.isfile(project['temp_path'] +
                              LinkFiles.TEMP_MATCHED_FILE)


def test_save(project, ddp):
    """Tests if the execution results are saved"""
    ddp.load_data()
    ddp.run()
    ddp.save()

    assert ddp.total_entities is not None
    assert ddp.total_entities == 998
    assert os.path.isfile(project['output_root'] + 'deduped_data.csv')
    assert os.path.isfile(project['output_root'] + project['name'] +
                          '_summary.pdf')

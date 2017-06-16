import json
import os
import pandas as pd
import pytest

from cdilinker.linker.dedup import DeDeupProject


@pytest.fixture(scope="module")
def project():
    __location__ = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__)))
    with open(os.path.join(__location__, "data/test_jtst_dedup.json")) \
            as data_file:
        return json.load(data_file)


def test_init_none():
    """Ensure initialization does not proceed with empty JSON"""
    with pytest.raises(TypeError):
        DeDeupProject(None)


def test_init(project):
    """Ensure initialization sets fields correctly"""
    ddp = DeDeupProject(project)

    assert ddp.project_type == 'DEDUP'
    assert ddp.left_index == project['datasets'][0]['index_field']
    assert ddp.right_index == project['datasets'][0]['index_field']
    assert ddp.matched is None
    assert ddp.left_dtypes is None
    assert ddp.right_dtypes is None


def test_str(project):
    """Should not be throwing a JSONDecodeError"""
    json.loads(str(DeDeupProject(project)))


def test_save_linked_data(project):
    """Tests if the deduped matched file exists"""
    # TODO Add dynamic file directory checking and removal
    ddp = DeDeupProject(project)
    df = pd.DataFrame()
    ddp._save_linked_data(df)

    assert os.path.isfile(project['output_root'] + 'dedup_matched.csv')
    # This should not run if assertion fails
    os.remove(project['output_root'] + 'dedup_matched.csv')


def test_load_data(project):
    """Tests if the data is properly loaded"""
    ddp = DeDeupProject(project)
    ddp.load_data()

    assert ddp.left_columns is not None
    assert ddp.left_dtypes is not None
    assert ddp.left_dataset is not None


def test_run(project):
    """Tests if the task can be run"""
    ddp = DeDeupProject(project)
    ddp.load_data()
    ddp.run()

    assert not os.path.isfile(project['output_root'] + 'matched_temp.csv')
    assert ddp.steps is not None


def test_save(project):
    """Tests if the execution results are saved"""
    NotImplemented

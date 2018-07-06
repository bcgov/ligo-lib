import os
import pytest
import shutil

from linker.core.chunked_dedup import ChunkedDedup
from linker.core.files import LinkFiles
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
        if os.path.isfile(project['output_root'] + 'left_file.csv'):
            os.remove(project['output_root'] + 'left_file.csv')
        if os.path.isfile(project['temp_path'] +
                          LinkFiles.TEMP_DEDUP_ALL_SELECTED):
            os.remove(project['temp_path'] +
                      LinkFiles.TEMP_DEDUP_ALL_SELECTED)
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
    return ChunkedDedup(project)


def test_init_none():
    """Ensure initialization does not proceed with empty JSON"""
    with pytest.raises(TypeError):
        ChunkedDedup(None)


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


def test_load_data(ddp):
    """Tests if the data is properly loaded"""
    ddp.load_data()

    assert ddp.left_columns is not None
    assert len(ddp.left_columns) == 7
    assert ddp.left_dtypes is not None
    assert len(ddp.left_dtypes) == 7
    assert ddp.right_columns is not None
    assert len(ddp.right_columns) == 7
    assert ddp.right_dtypes is not None
    assert len(ddp.right_dtypes) == 7


def test_link_pairs(project, ddp):
    """Tests if link_pairs behaves as intended"""
    step = project['steps'][1]
    matched_file = project['temp_path'] + LinkFiles.MATCHED_RECORDS
    open(matched_file, 'w').close()

    ddp.load_data()
    ddp.pair_n_match(step=step['seq'],
                     link_method=step['linking_method'],
                     blocking=step['blocking_schema'],
                     linking=step['linking_schema'],
                     matched_file=matched_file)
    value = ddp.link_pairs()

    assert value is not None
    assert value == 0


def test_extract_rows(project, ddp):
    """Tests if linked records are removed from input data"""
    step = project['steps'][1]
    matched_file = project['temp_path'] + LinkFiles.MATCHED_RECORDS
    open(matched_file, 'w').close()
    linked_file = project['temp_path'] + LinkFiles.TEMP_ENTITIES_FILE

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

    assert not os.path.isfile(project['temp_path'] +
                              LinkFiles.TEMP_STEP_REMAINED)
    assert os.path.isfile(ddp.left_file)
    assert Utils.file_len(ddp.left_file) == 1000


def test_run(project, ddp):
    """Tests if the task can be run"""
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

    assert not os.path.isfile(project['temp_path'] +
                              LinkFiles.MATCHED_RECORDS)
    assert not os.path.isfile(project['temp_path'] +
                              LinkFiles.TEMP_ENTITIES_FILE)
    assert not os.path.isfile(project['temp_path'] +
                              LinkFiles.TEMP_DEDUP_STEP_SELECTED)
    assert os.path.isfile(project['temp_path'] +
                          LinkFiles.TEMP_DEDUP_ALL_SELECTED)


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

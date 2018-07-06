import os
import pytest
import shutil

from linker.core.files import LinkFiles
from linker.core.memory_link import MemoryLink
from test.linker.utils import Utils


@pytest.fixture(scope="module")
def project():
    """Read test_jtst_educ_linking project configuration"""
    return Utils.load_project_data('test_jtst_educ_linking.json')


@pytest.fixture
def linker(request, project):
    if not os.path.exists(project['temp_path']):
        os.makedirs(project['temp_path'])

    def teardown():
        if os.path.isfile(project['temp_path'] +
                          LinkFiles.TEMP_MATCHED_FILE):
            os.remove(project['temp_path'] + LinkFiles.TEMP_MATCHED_FILE)
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

    request.addfinalizer(teardown)
    return MemoryLink(project)

def test_init_none():
    """Ensure initialization does not proceed with empty JSON"""
    with pytest.raises(TypeError):
        MemoryLink(None)


def test_init(project, linker):
    """Ensure initialization sets fields correctly"""
    assert linker.matched_not_linked is None
    assert linker.project_type == 'LINK'
    assert linker.left_index == project['datasets'][0]['index_field']
    assert linker.right_index == project['datasets'][1]['index_field']
    assert linker.left_entity == project['datasets'][0]['entity_field']
    assert linker.right_entity == project['datasets'][1]['entity_field']


def test_str(linker):
    """Should not be throwing a JSONDecodeError"""
    import json

    json.loads(str(linker))


def test_load_data(project, linker):
    """Tests if the data is properly loaded"""
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


def test_link(project, linker):
    """Tests link and filter functionality"""
    step = project['steps'][0]

    linker.load_data()
    linker.pair_n_match(step=step['seq'],
                        link_method=step['linking_method'],
                        blocking=step['blocking_schema'],
                        linking=step['linking_schema'])

    assert os.path.isfile(project['temp_path'] +
                          LinkFiles.TEMP_MATCHED_FILE)

    step_linked, step_matched_not_linked = \
        linker.link(step['seq'], project['relationship_type'])

    assert step_linked is not None
    assert len(step_linked) == 72
    assert step_matched_not_linked is not None
    assert len(step_matched_not_linked) == 0


def test_run(project, linker):
    """Tests if the task can be run"""
    linker.load_data()
    linker.run()

    assert linker.steps is not None
    assert len(linker.steps) == len(project['steps'])
    assert linker.total_records_linked == 144
    assert linker.total_entities == 30
    assert linker.linked is not None
    assert len(linker.linked) == 72
    assert not os.path.isfile(project['temp_path'] +
                              LinkFiles.TEMP_MATCHED_FILE)


def test_save(project, linker):
    """Tests if the execution results are saved"""
    linker.load_data()
    linker.run()
    linker.save()

    assert linker.total_entities is not None
    assert linker.total_entities == 30
    assert os.path.isfile(project['output_root'] +
                          'matched_not_linked_data.csv')
    assert os.path.isfile(project['output_root'] + 'linked_data.csv')
    assert os.path.isfile(project['output_root'] + project['name'] +
                          '_summary.pdf')

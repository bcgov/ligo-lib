import json
import os
import pytest

from cdilinker.linker.dedup import DeDeupProject


@pytest.fixture(scope="module")
def project():
    __location__ = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__)))
    with open(os.path.join(__location__, "test_dedup.json")) as data_file:
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

import filecmp
import os
import pytest
import shutil

import linker.linker_core.link_json as lj
from test.linker.utils import Utils


@pytest.fixture(params=[
    'combination',
    'levenshtein',
    'soundex'
])
def context(request):
    work_path = os.path.join(os.path.dirname(__file__), request.param)
    project = Utils.load_json(os.path.join(work_path, 'test1.json'))
    task_uuid = project['task_uuid']

    def teardown():
        if os.path.isdir(os.path.join(work_path, task_uuid)):
            shutil.rmtree(os.path.join(work_path, task_uuid))
        if os.path.exists(project['temp_path']):
            shutil.rmtree(project['temp_path'])

    request.addfinalizer(teardown)
    return work_path, task_uuid


def test_functional_dedup(context):
    """Functional tests for de-duplication"""
    work_path, task_uuid = context
    lj.main(['-p', work_path + os.sep + 'test1.json'])

    assert filecmp.cmp(work_path + os.sep + 'results1.csv',
                       work_path + os.sep + task_uuid + os.sep +
                       'deduped_data.csv', shallow=False)

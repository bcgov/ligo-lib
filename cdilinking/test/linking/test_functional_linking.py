import filecmp
import os
import pytest
import shutil

import cdilinker.linker.link_json as lj
from cdilinking.test.cdilinker.utils import Utils


@pytest.fixture(params=[
    'levenshtein',
    pytest.param('combination', marks=pytest.mark.slow)
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


def test_functional_linking(context):
    """Functional tests for de-duplication"""
    work_path, task_uuid = context
    lj.main(['-p', work_path + os.sep + 'test1.json'])

    assert filecmp.cmp(work_path + os.sep + 'results1_linked_data.csv',
                       work_path + os.sep + task_uuid + os.sep +
                       'linked_data.csv', shallow=False)
    assert filecmp.cmp(
        work_path + os.sep + 'results1_matched_not_linked.csv',
        work_path + os.sep + task_uuid + os.sep +
        'matched_not_linked_data.csv', shallow=False)

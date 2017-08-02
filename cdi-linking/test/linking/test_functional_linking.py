import filecmp
import os
import pytest
import shutil

import cdilinker.linker.link_json as lj
from test.cdilinker.utils import Utils


class TestFunctionalLinking(object):
    @pytest.fixture(params=[
        'levenshtein',
        pytest.param('combination', marks=pytest.mark.slow)
    ])
    def context(self, request):
        work_path = os.path.join(os.path.dirname(__file__), request.param)
        project = Utils.load_json(os.path.join(work_path, 'test1.json'))
        task_uuid = project['task_uuid']
        yield (work_path, task_uuid)

        # Teardown
        if os.path.isdir(os.path.join(work_path, task_uuid)):
            shutil.rmtree(os.path.join(work_path, task_uuid))
        if os.path.exists(project['temp_path']):
            shutil.rmtree(project['temp_path'])

    def test_functional_linking(self, context):
        """Functional tests for de-duplication"""
        lj.main(['-p', context[0] + os.sep + 'test1.json'])

        assert filecmp.cmp(context[0] + os.sep + 'results1_linked_data.csv',
                           context[0] + os.sep + context[1] + os.sep +
                           'linked_data.csv', shallow=False)
        assert filecmp.cmp(
            context[0] + os.sep + 'results1_matched_not_linked.csv',
            context[0] + os.sep + context[1] + os.sep +
            'matched_not_linked_data.csv', shallow=False)

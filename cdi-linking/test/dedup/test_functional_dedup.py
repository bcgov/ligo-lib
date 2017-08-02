import filecmp
import os
import pytest
import shutil

import cdilinker.linker.link_json as lj
from test.cdilinker.utils import Utils


class TestFunctionalDedup(object):
    @pytest.fixture(params=[
        'combination',
        'levenshtein',
        'soundex'
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

    def test_functional_dedup(self, context):
        """Functional tests for de-duplication"""
        lj.main(['-p', context[0] + os.sep + 'test1.json'])

        assert filecmp.cmp(context[0] + os.sep + 'results1.csv',
                           context[0] + os.sep + context[1] + os.sep +
                           'deduped_data.csv', shallow=False)

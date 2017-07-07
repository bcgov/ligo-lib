import filecmp
import os
import pytest
import shutil

import cdilinker.linker.link_json as lj
from test.cdilinker.utils import Utils


class TestFunctionalDedup(object):
    @pytest.mark.parametrize('directory', [
        'combination',
        'levenshtein',
        'soundex'
    ])
    def test_functional_dedup(self, directory):
        """Functional tests for de-duplication"""
        work_path = os.path.join(os.path.dirname(__file__), directory)
        project = Utils.load_json(os.path.join(work_path, 'test1.json'))
        task_uuid = project['task_uuid']
        lj.main(['-p', work_path + os.sep + 'test1.json'])

        assert filecmp.cmp(work_path + os.sep + 'results1.csv',
                           work_path + os.sep + task_uuid + os.sep +
                           'deduped_data.csv', shallow=False)

        if os.path.isdir(os.path.join(work_path, task_uuid)):
            shutil.rmtree(os.path.join(work_path, task_uuid))
        if os.path.exists(project['temp_path']):
            shutil.rmtree(project['temp_path'])

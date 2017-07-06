import filecmp
import os
import pytest
import shutil

import cdilinker.linker.link_json as lj
from test.cdilinker.utils import Utils


class TestFunctionalLinking(object):
    @pytest.mark.parametrize('directory', [
        # 'combination',
        'levenshtein'
    ])
    def test_functional_linking(self, directory):
        work_path = os.path.join(os.path.dirname(__file__), directory)
        project = Utils.load_json(os.path.join(work_path, 'test1.json'))
        task_uuid = project['task_uuid']
        lj.main(['-p', work_path + '/test1.json'])

        assert filecmp.cmp(work_path + '/results1_linked_data.csv',
                           work_path + '/' + task_uuid + '/linked_data.csv',
                           shallow=False)
        assert filecmp.cmp(
            work_path + '/results1_matched_not_linked.csv',
            work_path + '/' + task_uuid + '/matched_not_linked_data.csv',
            shallow=False)

        if os.path.isdir(os.path.join(work_path, task_uuid)):
            shutil.rmtree(os.path.join(work_path, task_uuid))
        if os.path.exists(project['temp_path']):
            shutil.rmtree(project['temp_path'])

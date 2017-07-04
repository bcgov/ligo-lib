import filecmp
import json
import os
import shutil

import cdilinker.linker.link_json as lj


class TestFunctionalDedup(object):
    def test_dedup_combination(self):
        work_path = os.path.join(os.path.dirname(__file__), 'combination')
        lj.main(['-p', 'cdi-linking/test/dedup/combination/test1.json'])

        with open(os.path.join(work_path, 'test1.json')) as data_file:
            project = json.load(data_file)
            task_uuid = project['task_uuid']

        assert filecmp.cmp('cdi-linking/test/dedup/combination/result1.csv',
                           'cdi-linking/test/dedup/combination/' + task_uuid +
                           '/deduped_data.csv', shallow=False)

        if os.path.isdir(os.path.join(work_path, task_uuid)):
            shutil.rmtree(os.path.join(work_path, task_uuid))

    def test_dedup_levensthein(self):
        work_path = os.path.join(os.path.dirname(__file__), 'levensthein')
        lj.main(['-p', 'cdi-linking/test/dedup/levensthein/test1.json'])

        with open(os.path.join(work_path, 'test1.json')) as data_file:
            project = json.load(data_file)
            task_uuid = project['task_uuid']

        assert filecmp.cmp('cdi-linking/test/dedup/levensthein/results1.csv',
                           'cdi-linking/test/dedup/levensthein/' + task_uuid +
                           '/deduped_data.csv', shallow=False)

        if os.path.isdir(os.path.join(work_path, task_uuid)):
            shutil.rmtree(os.path.join(work_path, task_uuid))

    def test_dedup_soundex(self):
        work_path = os.path.join(os.path.dirname(__file__), 'soundex')
        lj.main(['-p', 'cdi-linking/test/dedup/soundex/test1.json'])

        with open(os.path.join(work_path, 'test1.json')) as data_file:
            project = json.load(data_file)
            task_uuid = project['task_uuid']

        assert filecmp.cmp('cdi-linking/test/dedup/soundex/results1.csv',
                           'cdi-linking/test/dedup/soundex/' + task_uuid +
                           '/deduped_data.csv', shallow=False)

        if os.path.isdir(os.path.join(work_path, task_uuid)):
            shutil.rmtree(os.path.join(work_path, task_uuid))

import os


class Utils(object):
    @staticmethod
    def file_len(fname):
        """Counts number of lines in file fname"""
        if not os.path.isfile(fname):
            return 0

        with open(fname) as f:
            for size, l in enumerate(f):
                pass
        return size + 1

    @staticmethod
    def load_project(json_file):
        """Parses and loads a JSON Project json_file"""
        import json
        import pandas as pd
        import uuid

        # Suppress SettingWithCopyWarning warnings from Pandas
        # https://stackoverflow.com/q/20625582
        pd.options.mode.chained_assignment = None  # default='warn'

        with open(os.path.join(os.path.dirname(__file__), 'data', json_file)) \
                as data_file:
            project = json.load(data_file)

        # Add task_uuid to this project
        project['task_uuid'] = uuid.uuid4().hex
        return project

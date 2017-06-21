import os
import subprocess
import numpy as np
import logging

from string import Template
from cdilinker.config.config import config
from cdilinker.plugins.field_category import FieldCategory


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


# Get linking configuration
link_config = config.get_section('LINKER')
CHUNK_SIZE = int(link_config.get('chunk_size') or '100000')


LINKING_RELATIONSHIPS = (
    ('1T1', 'One to One'),
    ('1TM', 'One to Many'),
    ('MT1', 'Many to One'),
)


COLUMN_TYPES = {
    "VARCHAR": object,
    "BOOLEAN": np.bool_,
    "REAL": np.float64,
    "INTEGER": np.int64,
    "CHAR": object,
    "TEXT": object,
}


LINKING_METHODS = {
    'DTR': 'Deterministic',
    'PRB': 'Probabilistic',
}

FIELD_CATEGORIES = [field_cat() for field_cat in FieldCategory.plugins]


def _save_pairs(file_path, data, append=False):
    data.replace(np.nan, '', regex=True)
    if not append:
        data.to_csv(file_path)
    else:
        with open(file_path, 'a') as f:
            data.to_csv(f, header=False)


def sort_csv(filename, appendfile, cols, types):

    work_dir = os.path.split(filename)[0]
    if work_dir and work_dir[-1] == '/':
        work_dir = work_dir[:-1]

    template_file = os.path.dirname(__file__) + '/sort_script_template.txt'
    template_script_file = open(template_file)
    sort_script = Template(template_script_file.read())
    import csv
    header = []
    with open(filename, 'r') as in_file:
        reader = csv.reader(in_file)
        header = next(reader)

    col_index = {key: index + 1 for index, key in enumerate(header)}

    bash_str = 'tail -n +2 {0} | split -l 10000 - file_chunk_'.format(filename)
    sort_cols = []
    for col in cols:
        sort_cols.append('-k {0},{0}{1}'.format(col_index[col], 'n' if types[col] == 'numeric' else ''))

    sort_cols = ' '.join(sort_cols)

    params = {
        'filename': filename,
        'sort_cols': sort_cols,
        'chunksize': CHUNK_SIZE,
        'appendfile': appendfile,
        'work_dir': work_dir
    }

    sort_script = sort_script.substitute(params)
    script_filename = os.path.join(work_dir, 'sort_script.sh')

    with open(script_filename, 'w') as script_file:
        script_file.write(sort_script)
    os.chmod(script_filename, 0o700)
    subprocess.call([script_filename])

    if os.path.isfile(script_filename):
        os.remove(script_filename)

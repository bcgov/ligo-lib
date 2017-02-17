from __future__ import absolute_import

CHUNK_SIZE = 50000
MAX_PAIRS_SIZE = 5000000

LINKING_RELATIONSHIPS = (
    ('1T1', 'One to One'),
    ('1TM', 'One to Many'),
    ('MT1', 'Many to One'),
)


def _save_pairs(file_path, data, append=False):
    if not append:
        data.to_csv(file_path)
    else:
        with open(file_path, 'a') as f:
            data.to_csv(f, header=False)

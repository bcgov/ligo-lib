from __future__ import print_function

from cdilinker.linker.base import CHUNK_SIZE
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def get_dataset_size(file_path):
    return sum(1 for row in open(file_path))

def execute_project(project):

    left_size = get_dataset_size(project['datasets'][0]['url'])
    if project['type'] == 'DEDUP':
        right_size = 0
    else:
        left_size = get_dataset_size(project['datasets'][0]['url'])
        right_size = get_dataset_size(project['datasets'][1]['url'])

    if left_size > CHUNK_SIZE or right_size > CHUNK_SIZE:
        import cdilinker.linker.chunked_dedup as dedup
        import cdilinker.linker.chunked_link as link
    else:
        import cdilinker.linker.dedup as dedup
        import cdilinker.linker.link as link

    if project['type'] == 'DEDUP':
        task = dedup.DeDeupProject(project)
    else:
        task = link.Linker(project)

    task.load_data()
    task.run()
    return task.save()

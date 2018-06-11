from cdilinker.linker.base import CHUNK_SIZE

from cdilinker.linker.memory_link import MemoryLink
from cdilinker.linker.memory_dedup import MemoryDedup
from cdilinker.linker.chunked_link import ChunkedLink
from cdilinker.linker.chunked_dedup import ChunkedDedup


def get_dataset_size(file_path):
    return sum(1 for _ in open(file_path))


class LinkerFactory:
    """
    Creates proper linker/de-duplication instance depending on the project type
    and the size of the input dataset(s).
    """

    @staticmethod
    def create_linker(project):
        left_size = get_dataset_size(project['datasets'][0]['url'])
        if project['type'] == 'DEDUP':
            if left_size > CHUNK_SIZE:
                task = ChunkedDedup(project)
            else:
                task = MemoryDedup(project)
        else:
            right_size = get_dataset_size(project['datasets'][1]['url'])

            if left_size > CHUNK_SIZE or right_size > CHUNK_SIZE:
                task = ChunkedLink(project)
            else:
                task = MemoryLink(project)

        return task

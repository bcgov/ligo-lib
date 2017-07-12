from cdilinker.linker.base import CHUNK_SIZE

import cdilinker.linker.link as mem_link
import cdilinker.linker.dedup as mem_dedup
import cdilinker.linker.chunked_link as chunked_link
import cdilinker.linker.chunked_dedup as chunked_dedup


def get_dataset_size(file_path):
    return sum(1 for row in open(file_path))


class LinkerFactory():

    """
    Creates proper linker/de-duplication instance depending on the project type
    and the size of the input dataset(s).
    """

    @staticmethod
    def create_linker(project):

        left_size = get_dataset_size(project['datasets'][0]['url'])
        if project['type'] == 'DEDUP':
            if left_size > CHUNK_SIZE:
                task = chunked_dedup.DeDupProject(project)
            else:
                task = mem_dedup.DeDupProject(project)
        else:
            right_size = get_dataset_size(project['datasets'][1]['url'])

            if left_size > CHUNK_SIZE or right_size > CHUNK_SIZE:
                task = chunked_link.Linker(project)
            else:
                task = mem_link.Linker(project)


        return task

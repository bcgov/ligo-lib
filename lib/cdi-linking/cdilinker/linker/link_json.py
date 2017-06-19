import os
import sys
import json
import getopt
#from cdilinker.config.config import config
from cdilinker.linker.commands import execute_project


import logging
logger = logging.getLogger(__name__)

def run_json(project_file):
    '''
    Loads and runs a linking project from a json file.
    :param project_file: The json file of the linking project.
    :return: Linking results summary as a pdf file.
    '''

    if not os.path.exists(project_file):
        logger.error("The project file {0} was not found.".format(project_file))
    else:
        with open(project_file) as json_file:
            project = json.load(json_file)
            logger.debug(project['type'])
            return execute_project(project)

def main(argv):
    try:
        opts, args = getopt.getopt(argv, "hp:", ["--help", "--project"])
    except getopt.GetoptError:
        logger.info('link_json.py -p <json file>')
        sys.exit(2)
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            logger.info('link_json.py -p <json file>')
            sys.exit()
        elif opt in ("-p", "--project"):
            run_json(arg)


if __name__ == "__main__":
    main(sys.argv[1:])

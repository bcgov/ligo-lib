"""
Reads and parse system configutation and properties from an ini config file.
The config file name can be provided through the PROOF_CFG environment variable
or it can be specified on creating the configuration object.
"""
import os
import configparser

import logging


logger = logging.getLogger(__name__)

config = None


class Configuration:
    def __init__(self, config_file):
        self.config_file = config_file or os.environ.get('PROOF_CFG')
        self.config = None

    def load_config(self):
        self.config = configparser.ConfigParser()
        self.config.read(self.config_file)

    def get_section(self, section):

        if section is None or not self.config.has_section(section):
            return None

        data = {}
        for key in self.config.options(section):
            data[key] = self.config.get(section, key)

        return data


def load_config():

    logging.config.fileConfig(os.path.dirname(__file__) + '/proof.ini')

    logger.info('Loading config options.')
    config = Configuration(os.path.dirname(__file__) + '/proof.ini')
    config.load_config()
    logger.info('Config is loaded.')

    return config

# Create the configuration object and provide the config file if not provided by the environment variable.
if config is None:
    config = load_config()

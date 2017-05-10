"""
Reads and parse system configutation and properties from an ini config file.
The config file name can be provided through the PROOF_CFG environment variable
or it can be specified on creating the configuration object.
"""
from __future__ import print_function

import os
import configparser
import cdilinker

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


# Create the configuration object and provide the config file if not provided by the environment variable.
if config is None:
    config = Configuration(os.path.dirname(cdilinker.__file__) + '/proof.ini')
    config.load_config()


import setuptools
from distutils.core import setup
setup(
  name = 'LigoLib',
  packages = ['cdi-linking','linking_ext'], 
  version = '0.1.2',
  description = 'PyPi pkg for Linking Library Ligo-lib',
  author = 'Suraiya Khan',
  author_email = 'suraiya.uvic@gmail.com',
  url = 'https://github.com/NovaVic/ligo-lib', # use the URL to the github repo
  keywords = ['Linking', 'Deduplication', 'Record Linkage'], # arbitrary keywords
  classifiers = [
    "Programming Language :: Python :: 3.6",
    "Topic :: Software Development :: Libraries :: Python Modules",
  ],
)

import os

from setuptools import setup
from cdilinker.version import version


def read(filename):
    file_path = os.path.join(os.path.dirname(__file__), filename)
    return open(file_path).read()


setup(
    name="cdilinker",
    version=version,
    author="Khalegh Mamakani",
    author_email="khalegh@highwaythreesolutions.com",

    platforms="any",

    # Description
    description="BCGOV Python toolkit for linking and de-duplication of csv data files",
    long_description=read('../readme.rst'),

    install_requires=[
        "jellyfish",
        "numpy",
        "numexpr",
        "pandas",
        "xhtml2pdf",
        "jinja2",
        "html5lib"
    ],
    include_package_data=True,
    packages=[
        'cdilinker',
        'cdilinker.linker',
        'cdilinker.plugins',
        'cdilinker.reports'
    ],
    package_dir={'cdilinker': 'cdilinker'},
    license='Apache-2.0'
)

import os

from setuptools import setup


def read(filename):
    file_path = os.path.join(os.path.dirname(__file__), filename)
    return open(file_path).read()


setup(
    name="cdilinker-ext",
    version='0.1',
    author="Khalegh Mamakani",
    author_email="khalegh@highwaythreesolutions.com",

    platforms="any",

    # Description
    description="BCGOV Python algorithms extension for linking framework",
    long_description=read('readme.rst'),

    install_requires=[
        "cdilinker>=0.3"
    ],
    include_package_data=True,
    packages=[
        'bcgov_linkext',
    ],
    package_dir={'bcgov_linkext': 'bcgov_linkext'},
    license='MIT',
    entry_points="""
        [bcgov.linking.plugins]
        lev_alg = bcgov_linkext.algorithms:Levenshtein
        jaro_alg = bcgov_linkext.algorithms:JaroWinkler
    """,
)

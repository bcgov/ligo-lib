Ligo Library (aka ligo-lib )
==================

|License|

**ligo-lib** is a Python library to support the Ligo data linking application ( see [Ligo](https://github.com/bcgov/ligo) ).

This library bundles and provides the algorithms necessary to support entity resolution associated with
deduplication and linking of administrative datasets.

Inspired by `Python Record Linkage Toolkit <https://github.com/J535D165/recordlinkage>`__, **Ligo** takes
advantage of `Pandas <http://pandas.pydata.org/>`__ for faster data manipulations.

Build
-----

.. image:: https://travis-ci.org/bcgov/ligo-lib.svg?branch=master
    :target: https://travis-ci.org/bcgov/ligo-lib


Dependencies
------------

The **ligo-lib** needs to be executed in a Unix environment because it utilizes some shell scripting commands.

Ligo-lib also depends on:

- Python (>=3.6)
- NumPy (>=1.13.1)
- Pandas (>=0.20.3)
- jellyfish (>=0.5.6)
- xhtml2pdf (>=0.2b1)
- jinja2 (>=2.9.6)


Installation
------------

To install the latest version of library from Test PyPI simply use:

.. code:: sh

    pip3 install --no-cache-dir --upgrade --extra-index-url https://testpypi.python.org/pypi LigoLib 

This one would try to get a specific version of LigoLib 

.. code:: sh

    pip3 install --no-cache-dir --extra-index-url https://testpypi.python.org/pypi LigoLib==0.5 


This one would not always try to see beyond what is previously cached    

.. code:: sh
    pip3 install --extra-index-url https://testpypi.python.org/pypi LigoLib   
 

Tests 
---------------

The **ligo-lib**  tests depend on:

- pytest>=3.2.0
- pytest-cov>=2.5.1
- coverage>=4.4.1

To test the source code of the library as a contributing developer go to the root directory level where you did git pull and  simply use:

.. code:: sh

    python3 setup.py test  

test is set as an alias to pytest ( as specified in the setup.cfg file)


By default, all tests are configured to run. However, some tests will take considerable amount of time.
To skip running the time consuming tests in the library, open the pytest.ini file then remove the comment symbol (#) from  the add options block with
 -m not slow option specified and comment out the options block that does not have the "-m not slow" option specified.  Save the ini file and then execute 
the above (python3 setup.py test) command:



Docker Environment
~~~~~~~~~~~~~~~~~~
#<FIXME: this section should be updated>
If you are on a Windows host, ensure your local repo is saving files in unix format (LF instead of CRLF).
The functional tests will fail if the test input CSV files are saved in Windows format.

To run the tests within a docker environment (such as when you are on a Windows host) run the following:

.. code:: sh

    docker build . -t datalinking_test
    docker run --rm -it datalinking_test

To enter and explore the Docker container directly do the following:

.. code:: sh

    docker run --rm -it datalinking_test bash


How to Use
----------

To use the library and run a linking/de-duplication project, you need to create your project json file.
Having a json project, you can apply the library by :

.. code:: python

    python -m ligo.linker.link_json -p <project-file>


De-Duplication Project
----------------------

The input of the linking code is a linking/de-duplication project which, is defined as a sequence of
linking/de-duplication steps. De-duplication/Linking projects are defined by json files.
Below are samples of de-duplication and linking project files :

.. code:: JSON

    {
      "comments": "",
      "description": "File for dedup",
      "datasets": [
        {
          "description": "Test1  data",
          "format": "CSV",
          "url": "ligo/test/dedup/combination/test1.csv",
          "title": "Test1 data",
          "entity_field": "REC_ID",
          "data_types": {
            "FAMILY_NAME": "VARCHAR",
            "CANADIAN_POSTAL_CODE": "VARCHAR",
            "FIRST_GIVEN_NAME": "VARCHAR",
            "COUNTRY": "VARCHAR",
            "REC_ID": "VARCHAR",
            "PREF_FIRST_GIVEN_NAME": "VARCHAR",
            "STREET_LINE_1": "VARCHAR",
            "PREF_SECOND_GIVEN_NAME": "VARCHAR",
            "PROVINCE_OR_STATE": "VARCHAR",
            "BIRTH_DATE": "VARCHAR",
            "PREF_FAMILY_NAME": "VARCHAR",
            "SECOND_GIVEN_NAME": "VARCHAR",
            "COMMUNITY_OR_LOCATION": "VARCHAR"
          },
          "index_field": "REC_ID",
          "columns": [
            "CANADIAN_POSTAL_CODE",
            "FIRST_GIVEN_NAME",
            "COUNTRY",
            "PREF_FAMILY_NAME",
            "PREF_FIRST_GIVEN_NAME",
            "STREET_LINE_1",
            "PROVINCE_OR_STATE",
            "BIRTH_DATE",
            "FAMILY_NAME",
            "REC_ID",
            "PREF_SECOND_GIVEN_NAME",
            "SECOND_GIVEN_NAME",
            "COMMUNITY_OR_LOCATION"
          ],
          "name": "FILE1"
        }
      ],
      "linked_url": "",
      "name": "test1",
      "output_root": "ligo/test/dedup/combination/",
      "temp_path": "temp/",
      "matched_url": "",
      "results_file": "test1_dedup_summary.pdf",
      "status": "READY",
      "type": "DEDUP",
      "steps": [
        {
          "group": true,
          "seq": 1,
          "blocking_schema": {
            "right": [],
            "transformations": [
              "EXACT",
              "EXACT",
              "EXACT",
              "EXACT"
            ],
            "left": [
              "BIRTH_DATE",
              "FAMILY_NAME",
              "CANADIAN_POSTAL_CODE",
              "COMMUNITY_OR_LOCATION"
            ]
          },
          "linking_schema": {
            "comparisons": [
              {
                "name": "NYSIIS"
              }
            ],
            "right": [],
            "left": [
              "FIRST_GIVEN_NAME"
            ]
          },
          "linking_method": "DTR"
        },
        {
          "group": true,
          "seq": 2,
          "blocking_schema": {
            "right": [],
            "transformations": [
              "EXACT",
              "EXACT"
            ],
            "left": [
              "BIRTH_DATE",
              "CANADIAN_POSTAL_CODE"
            ]
          },
          "linking_schema": {
            "comparisons": [
              {
                "args": {
                  "n": 4
                },
                "name": "HEAD_MATCH"
              },
              {
                "args": {
                  "n": 4
                },
                "name": "HEAD_MATCH"
              }
            ],
            "right": [],
            "left": [
              "PREF_SECOND_GIVEN_NAME"
            ]
          },
          "linking_method": "DTR"
        }
      ]
    }


A De-duplication project consists of the input data file and a set of de-duplication steps.
The input datafile definition includes the path(URL) to data file,
name and title and the index field that uniquely identifies each record in the file.

Each de-duplication step includes the specification of blocking and linking variables and the transformation/comparison
algorithms. The blocking variables are used to reduce the comparison space and find potential record pairs.
The linking schema specifies the variables the must be compared by the corresponding comparison algorithms to find
records that belong to the same entities.

The group flag in each step indicates if the matched records will be grouped as a single entity and removed from
the input file or not. If the flag is false then not entity identifier will be generated for the matched records and
they will used in next de-duplication step. Otherwise, the matched records will be grouped and assigned the same entity
id and the records will be removed from the input file.

The outputs of a de-duplication project are :

*   De-duplicated output file with the new ENTITY_ID column. All the records that belong to the same entity will be assigned same entity id. The file is sorted by entity id.

*   De-duplication summary report as a pdf file.

*   De-duplication detailed output that indicates the records are linked and the first step at which they are linked.


Linking Project Project
-----------------------

.. code:: JSON

    {
      "status": "READY",
      "matched_url": "",
      "datasets": [
        {
          "description": "Education de-duplicated dataset",
          "format": "CSV",
          "url": "cdi-linking/test/linking/combination/educ_deduped.csv",
          "title": "De-depulicated dataset",
          "entity_field": "ENTITY_ID",
          "data_types": {
            "INGESTION_ID": "INTEGER",
            "FAMILY_NAME": "VARCHAR",
            "ENTITY_ID": "INTEGER",
            "CANADIAN_POSTAL_CODE": "VARCHAR",
            "FIRST_GIVEN_NAME": "VARCHAR",
            "REC_ID": "VARCHAR",
            "BIRTH_DATE": "VARCHAR",
            "SECOND_GIVEN_NAME": "VARCHAR"
          },
          "index_field": "INGESTION_ID",
          "columns": [
            "INGESTION_ID",
            "FAMILY_NAME",
            "ENTITY_ID",
            "CANADIAN_POSTAL_CODE",
            "FIRST_GIVEN_NAME",
            "REC_ID",
            "BIRTH_DATE",
            "SECOND_GIVEN_NAME"
          ],
          "field_cats": {
            "INGESTION_ID": "",
            "FAMILY_NAME": "",
            "ENTITY_ID": "",
            "CANADIAN_POSTAL_CODE": "",
            "FIRST_GIVEN_NAME": "",
            "REC_ID": "",
            "BIRTH_DATE": "",
            "SECOND_GIVEN_NAME": ""
          },
          "name": "Education_Deduped"
        },
        {
          "description": "TST Deduped dataset",
          "format": "CSV",
          "url": "ligo/test/linking/combination/tst_deduped.csv",
          "title": "TST Deduped dataset",
          "entity_field": "ENTITY_ID",
          "data_types": {
            "INGESTION_ID": "INTEGER",
            "ENTITY_ID": "INTEGER",
            "POSTAL_TXT": "VARCHAR",
            "FIRST_NAME_TXT": "VARCHAR",
            "REC_ID": "VARCHAR",
            "LAST_NAME_TXT": "VARCHAR",
            "BIRTH_DT": "VARCHAR"
          },
          "index_field": "INGESTION_ID",
          "columns": [
            "INGESTION_ID",
            "ENTITY_ID",
            "FIRST_NAME_TXT",
            "POSTAL_TXT",
            "REC_ID",
            "LAST_NAME_TXT",
            "BIRTH_DT"
          ],
          "field_cats": {
            "INGESTION_ID": "",
            "ENTITY_ID": "",
            "FIRST_NAME_TXT": "",
            "POSTAL_TXT": "",
            "REC_ID": "",
            "LAST_NAME_TXT": "",
            "BIRTH_DT": ""
          },
          "name": "TST_DEDUPED"
        }
      ],
      "description": "TST data linking",
      "linked_url": "",
      "comments": "Integer column has NA values in column 17",
      "output_root": "ligo/test/linking/combination/",
      "temp_path": "temp/",
      "results_file": "tst_summary.pdf",
      "steps": [
        {
          "group": false,
          "seq": 1,
          "blocking_schema": {
            "right": [
              "BIRTH_DT",
              "FIRST_NAME_TXT"
            ],
            "transformations": [
              "EXACT",
              "SOUNDEX"
            ],
            "left": [
              "BIRTH_DATE",
              "FIRST_GIVEN_NAME"
            ]
          },
          "linking_schema": {
            "comparisons": [
              {
                "args": {
                  "max_edits": 2
                },
                "name": "LEVENSHTEIN"
              },
              {
                "name": "EXACT"
              }
            ],
            "right": [
              "LAST_NAME_TXT",
              "POSTAL_TXT"
            ],
            "left": [
              "FAMILY_NAME",
              "CANADIAN_POSTAL_CODE"
            ]
          },
          "linking_method": "DTR"
        },
        {
          "group": false,
          "seq": 2,
          "blocking_schema": {
            "right": [
              "POSTAL_TXT",
              "LAST_NAME_TXT"
            ],
            "transformations": [
              "EXACT",
              "SOUNDEX"
            ],
            "left": [
              "CANADIAN_POSTAL_CODE",
              "FAMILY_NAME"
            ]
          },
          "linking_schema": {
            "comparisons": [
              {
                "args": {
                  "max_edits": 2
                },
                "name": "LEVENSHTEIN"
              },
              {
                "args": {
                  "n": 1
                },
                "name": "HEAD_MATCH"
              }
            ],
            "right": [
              "FIRST_NAME_TXT"
            ],
            "left": [
              "FIRST_GIVEN_NAME"
            ]
          },
          "linking_method": "DTR"
        }
      ],
      "relationship_type": "1T1",
      "type": "LINK",
      "name": "tst"
    }


A linking project is defined by:

*   Datasets. These are the files to be linked.

*   Type of entity relationship. This defines how entities relate to each other:

    1. 1T1 : one-to-one
    2. 1TM: one-to-many
    3. MT1: many-to-one


*   Linking steps

Each linking step is defined by:

*   Selection of blocking variables. This defines the size of the search space
*   Selection of linking variables. This defines the comparison space
*   Selection of comparison operations to be performed on blocking and linking variables.


Blocking and Linking Variables
------------------------------

In general, a variable could function as a blocking or linking variable or both; this functionality may change from one
step to another. In order words, a variable could be a blocking variable or a linking variable or both
(e.g., blocking: Soundex of first name; linking: jaro-winkler of first name) within a step and this might change in
a different linking step.


The linking process generates the following output files:

*   Linking summary pdf report.

*   Linked output file. This file contains information about linked entities. It also describes the linking step where said entities were linked.

*   Matched_but_not_linked file. This file contains information about matched entities that were not linked due to conflicts on the type-of-relationship.


    Copyright 2018 Province of British Columbia

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

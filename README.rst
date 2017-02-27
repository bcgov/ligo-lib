Data Linking Framework
======================

**Data Linking** is a framework for defining and running de-duplication/linking projects to find and link records that belong to the
same individual(entity) in a single or multiple files.

Features
--------

    - `Django <https://www.djangoproject.com/>`__-based web app for manging de-duplication/Linking projects.
    - Uses `Celery <http://www.celeryproject.org/>`__ for executing projects asynchronously.
    - Multi-step linking/de-duplication project creation. Each step comes with its own blocking/linking variables and comparison algorithms.
    - Supports a wide range of comparison algorithms.
    - Exports linking projects to json. The json files can be used to run projects from command line.




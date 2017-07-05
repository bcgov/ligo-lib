CDI Linking
===========

A web application for linking multiple datasets.

.. image:: https://img.shields.io/badge/built%20with-Cookiecutter%20Django-ff69b4.svg
     :target: https://github.com/pydanny/cookiecutter-django/
     :alt: Built with Cookiecutter Django


:License: MIT


Settings
--------

The following environment variables are required for project settings:
    - DJANGO_SECRET_KEY : Django installation secret key.
    - DATABASE_URL : postgres://<user>:<password>@<host>:<port>/linkage
    - CELERY_BROKER_URL : For example, redis://localhost:6379/0
    - LINKING_DATASTORE_URL : Path to dataset store directory
    - LINKING_OUTPUT_URL : Path to linking output directory
    - DJANGO_DEBUG : True/False, Toggles debug mode

    - LINK_DB_USER: postgres database user name
    - LINK_DB_HOST: postgres hostname or url
    - LINK_DB_PORT: postgres port
    - LINK_DB_SERVICE: postgres service name
    - LINK_DB_PASSWORD: password for Postgres Database

    - IN_DOCKER : valid value 0 or 1
    - C_FORCE_ROOT: celery should run as root or non root
                boolean; valid value : true/false


    # General settings
    DJANGO_ADMIN_URL=
    DJANGO_SETTINGS_MODULE=config.settings.local
    DJANGO_SECRET_KEY=e(q8huau(-+qz6oi9!k62#_+t61n(*7daz7vmv2439ns1+2=g^
    DJANGO_ALLOWED_HOSTS=.localhost  pass this as env variable

    You may choose to modify code and pass INTERNAL_IPS as environment variable
    as well

Generate Config files
----------------------
    We have a python script called transform.py that takes
    (1) desired config values (that is going to change based on system of
    interest like dev/qa/staging/uat/prod) in a file (the file name mentioned
     after --input in call syntax)
    (2) a skeleton/template file on which the desired config values will be stashed
    (3) projectroot where the template file is located and
    (4) location and name for the output/actual environment/system specific config file

    Inside the input file (as mentioned after the --input args) specify the values
    as appropriate for your system of interest. And please make sure that in each
     of the relevant input files (containing input config values) the value for
     deploymentconfig matches what you are going to specify after --output (have a
     look at the call syntax below). This is required to make sure that service/routing
      etc. files as linked to appropriate deployment.

    Go to data-linking directory then
    if you want to generate deploymentconfig for django then copy
    sample input file from
    data-linking\web\linkage\config\openshift\sampleinput\web\deployment.input.sample
    into a file called data-linking\web\linkage\config\openshift\sampleinput\web\deployment.input
     (as for example). You will use this new file after the --input args for the
     transformation process.  Update the content in this file as appropriate
    When you are generating config file for postgres, redis etc. go to corresponding
    directories (data-linking\web\linkage\config\openshift\sampleinput\postgres,
    data-linking\web\linkage\config\openshift\sampleinput\redis) and use appropriate
    input files.

    The template file for django is in data-linking\web\linkage\config\openshift\template\web;
    we call this location as projectroot in the following command
    Use data-linking\web\linkage\config\openshift\template\postgres or
    data-linking\web\linkage\config\openshift\template\redis for configuration skeleton/
    template for postgres or redis

    Call syntax:
    python transform.py  --input "C:\Users\Suraiya\khalegh_linking_latest_2\data-linking\web\linkage\config\openshift\sampleinput\web\deployment.sample.input"   --output djangodeploymentconfig.yml --template deployment.yaml.tpl --projectroot "C:\Users\Suraiya\khalegh_linking_latest_2\data-linking\web\linkage\config\openshift\template\web"

    In the above command transform.py is considering the filename after --input as a file
    containing desired configuration values for the system where you are going to deploy
    the app.

    The filename (filename containing both full path and filename) that is appearing
    after the --output param is the location where the generated config (in this case django
    deployment config for the openshift env) file will be saved.

    The path that appears after --projectroot is the location of the template/skeleton
    file which will be used to generate the actual config file.

    The value after --template is the file name  (here deployment.yaml.tpl that is
    available immediately under the project root) and does not contain full path. This is
    the template file that defines the structure of output config file.


Basic Commands
--------------

Setting Up Your Users
^^^^^^^^^^^^^^^^^^^^^

* To create a **normal user account**, just go to Sign Up and fill out the form. Once you submit it, you'll see a "Verify Your E-mail Address" page. Go to your console to see a simulated email verification message. Copy the link into your browser. Now the user's email should be verified and ready to go.

* To create an **superuser account**, use this command::

    $ python manage.py createsuperuser

For convenience, you can keep your normal user logged in on Chrome and your superuser logged in on Firefox (or similar), so that you can see how the site behaves for both kinds of users.

Deployment
----------

Installing dependencies:
^^^^^^^^^^^^^^^^^^^^^^^

Use the following command in application root directory to install all required python libraries:

.. code:: sh

    pip install -r /requirements/local.txt

For production env use ;

.. code:: sh

    pip install -r /requirements/production.txt


Database Migration :
^^^^^^^^^^^^^^^^^^^^

linking web application uses postgresql linkage database for managing datasets and linking projects.


To migrate database, on the application root directory run :

.. code:: python

    python manage.py migrate



Celery
^^^^^^

The web application uses Celery to run linking jobs asynchronously.
You need to setup a Celery broker like Redis or
RabbitMQ and start a Celery worker.

The Celery broker is provided by CELERY_BROKER_URL environment variable. To start a Celery worker use:

.. code:: sh

    celery -A linkage worker --loglevel=INFO

Docker Based Deployment
^^^^^^^^^^^^^^^^^^^^^^^
For docker-compose based solution follow
use

docker-compose up --build --force-recreate

Openshift and Kubernetes Based Deployment
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
1) Build using Decker-dev for solution where django
user is not in sudoers list

Also pass start-dev.sh to entrypoint.sh

2) Build using Docker-dev-os for solution where the django
 user is in sudoers list

Also pass start-dev-os.sh to entrypoint.sh

It may be a good idea to rearrange the content of parent directory
and split content of this file into separate readme files.

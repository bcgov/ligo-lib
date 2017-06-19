#!/bin/sh
#non elegant solution but allows to read from mounted volume
#JIRA - 762
#pass this one to docker entry point if you are ok with
#non root being in sudoers list and want to read successfully
#from the mounted volume
#if you are using this file then you are supposed to use Dockerfile-dev-os
#instead of Dockerfile-dev

sudo chmod -R 777 /user_data
sudo chown -R django:django /user_data
celery -A linkage.taskapp worker -l DEBUG &

python manage.py makemigrations
python manage.py migrate
#this 8000 could be passed from env file too.
python manage.py runserver_plus 0.0.0.0:8000

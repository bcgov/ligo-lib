#!/bin/sh
# #comment out or one or the other depending on what we want to list
# #debug level message or info level message
#   #ldeally info > debug
#   #if we want to capture a msg at level y and any level higher than that
#   #then we have to set the level
#   #to the lower one (y) after the -l flag
#   #command: celery -A linkage.taskapp worker -l INFO
#   command: celery -A linkage.taskapp worker -l DEBUG

celery -A linkage.taskapp worker -l DEBUG &
python manage.py makemigrations
python manage.py migrate
#this 8000 could be passed from env file too.
python manage.py runserver_plus 0.0.0.0:8000

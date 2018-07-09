FROM python:3.6
ENV PYTHONUNBUFFERED 1

#Docker file that pulls LigoLib package from PyPI (instead of copying source code) and
#build docker image for celery worker(s)

# Docker file for forming local/dev Django container
RUN mkdir -p /user_data/media/datasets /user_data/media/linking
RUN mkdir /tmp/django-email-dev
# Ownership change not a requirement for local/dev docker environments
RUN chmod -R 0777 /user_data

# Install Celery Requirements
COPY ./link-server /worker
RUN pip install -r /worker/requirements.txt

# Install Data Linking Packages

#this one would not always try to see beyond what is previously cached
#RUN pip install --extra-index-url https://testpypi.python.org/pypi LigoLib

#this one would try to get the latest
RUN pip3 install --no-cache-dir --upgrade --index-url https://testpypi.python.org/pypi LigoLib

#this one would try to get a specific version of LigoLib
#RUN pip3 install --no-cache-dir  --extra-index-url https://testpypi.python.org/pypi  LigoLib==3.0.5

RUN echo 'django:x:999:999:django:/:/bin/sh' >> /etc/passwd

RUN chown 999:999 -R /worker
RUN chown 999:999 -R /user_data
RUN chown 999:999 -R /tmp

USER "999:999"
WORKDIR /worker

ENTRYPOINT ["celery", "-A", "tasks", "worker", "--loglevel=info"]

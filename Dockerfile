# Dockerfile for providing a test environment for Windows developers or contributing
#developers so that they can modify source code of Ligo-lib, build docker image from
#source code and test correctness

FROM python:3.6
ENV PYTHONUNBUFFERED 1

## Requirements have to be pulled and installed here, otherwise caching won't work
#COPY ./requirements /usr/src/app/requirements
#RUN pip install -r /usr/src/app/requirements/test.txt

COPY  . /usr/src/app/
WORKDIR /usr/src/app

RUN pip install -r /usr/src/app/requirements/test.txt

CMD chmod 755 /usr/src/app/setup.py


# Default container action - run Pytest tests
#the following would run the pytest
CMD ["python3", "setup.py", "test"]

# Dockerfile for providing a test environment for Windows developers
FROM python:3.6
ENV PYTHONUNBUFFERED 1

# Requirements have to be pulled and installed here, otherwise caching won't work
COPY ./requirements /usr/src/app/requirements
RUN pip install -r /usr/src/app/requirements/test.txt

# Install cdi-linking
COPY ./cdi-linking /usr/src/app/cdi-linking
# The cdi-linking requires the readme.rst file to be one directory above itself
COPY ./readme.rst /usr/src/app/readme.rst
RUN pip install -e /usr/src/app/cdi-linking

# Install linking_ext
COPY ./linking_ext /usr/src/app/linking_ext
RUN pip install -e /usr/src/app/linking_ext

# Set up Test environment
COPY ./pytest.ini /usr/src/app/pytest.ini
COPY ./pylintrc /usr/src/app/pylintrc
WORKDIR /usr/src/app

# Default container action - run Pytest tests
CMD ["pytest"]

# Dockerfile for providing a test environment for Windows developers

FROM python:3.6
ENV PYTHONUNBUFFERED 1

# Requirements have to be pulled and installed here, otherwise caching won't work
COPY ./requirements /data-linking/requirements
RUN pip install -r /data-linking/requirements/test.txt

# Install cdi-linking
COPY ./cdi-linking /data-linking/cdi-linking
# The cdi-linking requires the readme.rst file to be one directory above itself
COPY ./readme.rst /data-linking/readme.rst
RUN pip install -e /data-linking/cdi-linking

# Install linking_ext
COPY ./linking_ext /data-linking/linking_ext
RUN pip install -e /data-linking/linking_ext

# Set up Test environment
COPY ./pytest.ini /data-linking/pytest.ini
COPY ./pylintrc /data-linking/pylintrc
WORKDIR /data-linking

# Default container action - run Pytest tests
CMD ["pytest"]
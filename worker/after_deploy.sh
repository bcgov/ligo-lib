#!/bin/bash
#to build docker image from PyPI package, tagging it and then pushing it to dockerhub
echo "$DOCKERHUB_PASSWORD" | docker login https://index.docker.io/v1/ -u "$DOCKERHUB_USERNAME" --password-stdin

#experimental passing of environment variable from travis env
#if that does not work then we can go for traditional ways of retrieving it using git describe
docker build --build-arg LIB_VERSION=$LIB_VERSION ./worker -t  novavic/ligo_celeryworker:$LIB_VERSION

#docker tag ligo_celeryworker novavic/ligo_celeryworker:$(git describe --always)
docker push novavic/ligo_celeryworker:$LIB_VERSION

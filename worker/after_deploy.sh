#!/bin/bash
#to build docker image from PyPI package, tagging it and then pushing it to dockerhub
echo "$DOCKERHUB_PASSWORD" | docker login https://index.docker.io/v1/ -u "$DOCKERHUB_USERNAME" --password-stdin

#docker build  ./worker -t  novavic/ligo_celeryworker:$(git describe --always)

docker tag ligo_celeryworker novavic/ligo_celeryworker:$(git describe --always)
docker push novavic/ligo_celeryworker:$(git describe --always)

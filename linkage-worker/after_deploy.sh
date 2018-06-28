#!/bin/bash
echo "$DOCKERHUB_PASSWORD" | docker login https://index.docker.io/v1/ -u "$DOCKERHUB_USERNAME" --password-stdin

docker build . -t novavic/ligo_celeryworker

#docker tag ligo_celeryworker novavic/ligo_celeryworker
docker push novavic/ligo_celeryworker:latest

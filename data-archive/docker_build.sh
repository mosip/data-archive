#!/bin/sh
# ./docker_build.sh git_tag
TAG=mosipdev/data-archive:1.0.7
echo Building $TAG
docker build --no-cache -t $TAG .

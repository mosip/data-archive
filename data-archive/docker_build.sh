#!/bin/sh
# ./docker_build.sh git_tag
TAG=bn46/data-archive:1.0.0
echo Building $TAG
docker build --no-cache -t $TAG .

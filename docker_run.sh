#!/bin/bash

docker ps -a | awk '{print $1}' | xargs docker rm -f
docker run -d -p 32200:22 -p 9001-9002:9001-9002 -p 19600-19610:19600-19610 -p19999:19999 --name bench ywx217/mq-benchmark

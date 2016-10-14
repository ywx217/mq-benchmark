#!/bin/bash

docker ps -a | awk '{print $1}' | xargs docker rm -f
docker run -d -p 32200:22 -p 9001-9002:9001-9002 -p 19600-20000:19600-20000 --name bench ywx217/mq-benchmark

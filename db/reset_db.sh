#!/bin/bash

docker-compose down
docker container rm db_db_1
yes | docker image rm postgres:latest
yes | docker image rm postgres:10
sudo rm -rfv db_home/
docker-compose up -d

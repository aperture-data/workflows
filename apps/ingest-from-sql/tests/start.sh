#!/bin/bash
if [[ "${SQL_SERVER}" == "mysql" ]];
then
    docker compose -f mysql-compose.yml up -d
else
    docker compose -f postgres-compose.yml up -d
fi

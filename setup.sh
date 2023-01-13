#!/bin/bash

# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source `pwd`/venv/bin/activate

# install requirements
pip install -r requirements.txt

# setup the enviroment variables

touch .env

echo "POSTGRES_PORT='5440'" >> .env
echo "POSTGRES_DBNAME='mydb'" >> .env
echo "POSTGRES_USER='postgres'" >> .env
echo "POSTGRES_PASSWORD='mypassword'" >> .env
echo "POSTGRES_HOST='127.0.0.1'" >> .env

# Pull the latest Postgres image
docker pull postgres

# Start a new container and set a password for the default "postgres" user
docker run --name my-postgres -e POSTGRES_PASSWORD=mypassword -d -p 5440:5432 postgres

echo 'container created'
sleep 5

# Connect to the running container and create a database
docker exec -it my-postgres sh -c 'createdb mydb -U postgres'

# Copy sql file for create tables
docker cp create_tables.sql my-postgres:/home

# Create tables
docker exec -it my-postgres psql -U postgres -d mydb -a -f /home/create_tables.sql

echo 'installation finished'

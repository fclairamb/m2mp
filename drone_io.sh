#!/bin/bash
./drone_io_start_cassandra.sh
./load_cql.sh

DIR=`pwd`

# Go
cd $DIR/go/m2mp-db
go get
go test -v -timeout 10s

cd $DIR/go/m2mp-db/entities
go get
go test -v -timeout 10s

# Python
cd $DIR/python
sudo apt-get install build-essential python-dev libev-dev
sudo pip install cassandra-driver
python tests.py

# Java
export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64/jre
cd $DIR/java/org.m2mp.db
mvn install


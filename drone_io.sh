#!/bin/bash
./drone_io_start_cassandra.sh
./load_cql.sh

DIR=`pwd`

# Go
cd $DIR/go/m2mpdb
go get
go test -v -timeout 10s

# Python
cd $DIR/python
python tests.py

# Java
cd $DIR/java/org.m2mp.db
mvn install


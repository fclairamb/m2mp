#!/bin/bash
./drone_io_start_cassandra.sh
./load_cql.sh

cd go/m2mpdb
go get
go test -v -timeout 10s

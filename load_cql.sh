#!/bin/bash
cd cql
for f in `ls`; do cat $f | $DIR/apache-cassandra-2.0.3/bin/cqlsh; done
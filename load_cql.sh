#!/bin/bash
for f in `ls cql`; do cat $f | cassandra/bin/cqlsh; done

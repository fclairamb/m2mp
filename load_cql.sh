#!/bin/bash
for f in `find cql -type f | sort`; do printf "Loading ${f}..." ; cat $f | cassandra/bin/cqlsh; echo " OK" ; done 

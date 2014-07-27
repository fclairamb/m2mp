#!/bin/bash
CQLSH=cqlsh

if [ -f cassandra/bin/cqlsh ]; then
  CQLSH=cassandra/bin/cqlsh
fi

for f in `find cql -type f | sort`; do printf "Loading ${f}..." ; cat $f | ${CQLSH} && echo " OK" ; done 

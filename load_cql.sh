#!/bin/bash
for f in `find cql -type f`; do cat $f | cassandra/bin/cqlsh; done

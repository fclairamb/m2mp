#!/bin/sh
export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64/jre

DIR=`pwd`

##################
# Cassandra 2.0.x
##################
curl -LO http://archive.apache.org/dist/cassandra/2.0.3/apache-cassandra-2.0.3-bin.tar.gz
tar xf apache-cassandra-2.0.3-bin.tar.gz
cd apache-cassandra-2.0.3
sudo mkdir -p /var/lib/cassandra /var/log/cassandra
sudo chown `whoami` /var/lib/cassandra /var/log/cassandra
bin/cassandra
cd $DIR
cd cql
# sleep 20
for i in {0..30}; do echo "Waiting server ($i)..." ; nc localhost 9042 </dev/null && break ; sleep 1; done;

for f in `ls`; do cat $f | $DIR/apache-cassandra-2.0.3/bin/cqlsh; done
cd $DIR
cd go/m2mpdb
go get
go test -v -timeout 10s

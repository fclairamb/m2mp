#!/bin/bash
export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64/jre

CASSANDRA_VERSION=2.1.1

curl -LO http://archive.apache.org/dist/cassandra/${CASSANDRA_VERSION}/apache-cassandra-${CASSANDRA_VERSION}-bin.tar.gz
tar xf apache-cassandra-${CASSANDRA_VERSION}-bin.tar.gz
ln -s apache-cassandra-${CASSANDRA_VERSION} cassandra
cd cassandra

sudo mkdir -p /var/lib/cassandra /var/log/cassandra
sudo chown `whoami` /var/lib/cassandra /var/log/cassandra

bin/cassandra

for i in {0..30}; do echo "Waiting server ($i)..." ; nc localhost 9042 </dev/null && exit 0 ; sleep 1; done;

exit 1

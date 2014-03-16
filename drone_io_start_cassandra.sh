#!/bin/bash
export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64/jre

curl -LO http://archive.apache.org/dist/cassandra/2.0.5/apache-cassandra-2.0.5-bin.tar.gz
tar xf apache-cassandra-2.0.5-bin.tar.gz
ln -s apache-cassandra-2.0.5 cassandra
cd cassandra

sudo mkdir -p /var/lib/cassandra /var/log/cassandra
sudo chown `whoami` /var/lib/cassandra /var/log/cassandra

bin/cassandra

for i in {0..30}; do echo "Waiting server ($i)..." ; nc localhost 9042 </dev/null && exit 0 ; sleep 1; done;

exit 1

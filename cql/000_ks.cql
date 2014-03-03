drop keyspace ks_test;

CREATE KEYSPACE ks_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};


M2M platform
============

It's the database setup I have used for few projects on top of cassandra. It contains three parts:

* Timeseries handling
* RegistryNode : Some kind of registry to organized data around a not-so-synced database. 
The goal is to make sure the data is easy to understand, it can be shared between any language and different versions
of the same system.
* RegistryNodeData : Only implemented on java. It allows to store any number of files on this database by adding
a column family for storing data chunks.

M2M platform
============

It's the database setup I have used for few projects on top of cassandra. It contains three parts:

* Time series handling
* A registry
* Binary content storage on top of the registry (only in java)


Time series
-----------
Data around time series is regrouped by a period a time, the current one being a month.

A data stored in the TS contains: 
* An id: The thing (a tracker)
* A type: The component (a location)
* A date: When it was taken
* Some data: coordinates + speed


Registry
--------
The registry is working like a filesystem, except files are values. 
For example, data about a device should be "/device/<device UUID>". Data about an user should be in "/user/<user UUID>".

It is **NOT** optimized for moving nodes, it means we have to copy everything if we want to do that. It is instead optimized for multi-server synchronization (merging) of data and easy recovery.


Registry data
-------------
This allows to store binary content (like files) on top of the registry. It saves data by chunks. 
It's perfect to host a huge amount of files.


Entities
--------
The java implementation has two entities (User and Domain) which should be used as base classes. They
simply are a way to make it easier to handle data.

They also provide a pattern to upgrade data based on a previous version of the same object.


Implementations
---------------
It contains three implementation:
* Java: Ready for action (used in production and unit test), no optimization has been done
* Python: Dirty + no registry data
* Go: Dirty + no registry data

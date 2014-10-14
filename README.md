[![Build Status](https://travis-ci.org/cmars/gonzodb.svg?branch=master)](https://travis-ci.org/cmars/gonzodb)

gonzodb
=======
An in-memory data store that speaks enough mongodb protocol to use in place of a live mongodb in your tests.

Planned Features
================
* Designed for testing applications that are hard-coded against the mgo driver.
* Implements enough of a subset of the mongodb protocol to fool mgo for most purposes.
* Easy to set up and tear down in unit tests.
* Can easily introspect or dump on contents after a test is run.

Status
======

DONE
----
* Can connect with mongo command line client, mgo driver.
* mgo-based test cases.
* Simple-case CRUD is working for query, insert, update, delete.
* Some database and admin commands are supported.

TODO
----
* More complete coverage of query operators (comparison, other matching)
* Sub-document selector matching, modification
* findAndModify

BACKLOG
-------
* Cursors
* Auth commands
* TLS
* Indexes
* Capped collections
* Backend refactoring
* Moar backends (PostgreSQL JSONB, Cassandra, Riak, etc.)

Resources
=========
* http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/

Other in-memory MongoDB protocol servers:
* https://github.com/bwaldvogel/mongo-java-server
* https://github.com/rick446/MongoTools/blob/master/mongotools/mim/mim.py

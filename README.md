gonzodb
=======
An in-memory data store that speaks enough mongodb protocol to use in place of a live mongodb in your tests.

Why?
====
* Testing against a live mongodb makes your tests fragile.
* Best-case, mocks only provide partial quality assurance while adding maintenance costs.
* Worst-case, they're a tautological waste of time, effort and code.

Features (planned)
==================
* Designed for testing applications that are hard-coded against the mgo driver.
* Implements enough of a subset of the mongodb protocol to fool mgo for most purposes.
* Easy to set up and tear down in unit tests.
* Can easily introspect or dump on contents after a test is run.

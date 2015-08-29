CDB
===

[![GoDoc](https://godoc.org/github.com/colinmarc/cdb/web?status.svg)](https://godoc.org/github.com/colinmarc/hdfs/web) [![build](https://travis-ci.org/colinmarc/cdb.svg?branch=master)](https://travis-ci.org/colinmarc/hdfs)

This native Go implementation of [cdb][1], a constant key/value database with
some very nice properties.

[1]: http://cr.yp.to/cdb.html

Usage
-----

```go
writer, err := cdb.Create("/tmp/example.cdb")
if err != nil {
  log.Fatal(err)
}

// Write some key/value pairs to the database.
writer.Put([]byte("Alice"), []byte("Practice"))
writer.Put([]byte("Bob"), []byte("Hope"))
writer.Put([]byte("Charlie"), []byte("Horse"))

// Freeze the database, and open it for reads.
db, err := writer.Freeze()
if err != nil {
  log.Fatal(err)
}

// Fetch a value.
v, err := db.Get([]byte("Alice"))
if err != nil {
  log.Fatal(err)
}

log.Println(string(v))
// => Practice
```

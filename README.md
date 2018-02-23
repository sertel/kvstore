# kvstore

A reference implementation for a key-value store that consists of several microservices.

Build:
```
stack build
```

Run:
```
stack exec kvstore
```

To re-generate the code for the Thrift part navigate to the `thrift` directory and run:
```
thrift -r --gen hs kvservice.thrift
thrift -r --gen hs db.thrift
```

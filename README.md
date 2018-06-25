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

Correctness Tests:
```
stack test
stack test
stack test
```

Micro Benchmark (need to/should be executed sequentially):

Reads a JSON config from stdin, writes results to stdout (also json)
```
stack exec -- microbench
```

The easiest way to generate configs is using Haskell, creating `BatchConfig`
values (in `test/MBConfig.hs`) and serializing them using `Aeson.encode`.
The format for the config is the `BatchConfig` fields with camel case replaced
with snake case.

Example:

```json
{
  "key_count": 100,
  "batch_count": 30,
  "batch_size": 30,
  "use_encryption": true,
  "num_tables": 20,
  "thread_count": 8,
  "systemVersion": "Imperative_coarseGrained"
}
```

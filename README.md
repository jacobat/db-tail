db-tail
=======

A small utility to tail the contents of a database table.

Currently it only supports [MessageDB](https://docs.eventide-project.org/user-guide/message-db/).

Installation
------------

```
cargo install db-tail
```

Usage
-----

```
Usage: db-tail [OPTIONS]

Options:
  -s, --stream-name-filter <STREAM_NAME_FILTER>  Stream name filter
  -h, --help                                     Print help
  -V, --version                                  Print version
```

The `--stream-name-filter` option is optional. If provided, only messages
from the named stream is shown. Multiple streams can be nmaed by providing
multiple `--stream-name-filter` options.

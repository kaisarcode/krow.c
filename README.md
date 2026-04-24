# krow

Persistent multi-value key-value store for unsigned 64-bit keys.

## Build

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build --config Release
```

## CLI

```bash
./krow ini data.krow 1000000
./krow set data.krow 123 "value"
./krow get data.krow 123
./krow del data.krow 123
./krow prune data.krow
```

All CLI errors return a non-zero status and write diagnostics to stderr.
Numeric arguments are parsed strictly.

## API

```c
#include "krow.h"

kc_krow_t *ctx = kc_krow_open("data.krow", 1000000);

kc_krow_set(ctx, 123, "value", 5);
kc_krow_get(ctx, 123, callback, arg);
kc_krow_del(ctx, 123);
kc_krow_prune(ctx);
kc_krow_sync(ctx);
kc_krow_close(ctx);
```

`kc_krow_open(path, 0)` opens an existing database only and never creates a
file. `kc_krow_open(path, capacity)` with `capacity > 0` creates a new
database only.

One `kc_krow_t` may be shared across threads. Public operations are serialized
inside the context with one mutex. `kc_krow_close` is also serialized and must
be the final operation on the context.

## Storage

krow uses an mmap-backed file with a versioned header, entry checksums, commit
markers, and full index rebuild during open-time recovery. `prune` writes a
compact temporary database, rebuilds the index from live records, syncs it, and
atomically replaces the original file.

The process model is exclusive access: only one process may open a database at
a time. POSIX sync uses `msync` and `fsync`; Windows sync uses
`FlushViewOfFile` and `FlushFileBuffers`.

## Tests

```bash
./test.sh
```

The test suite is functional. It covers init, set/get, multi-value records,
delete, collision prune, missing open, invalid numbers, full capacity, reopen,
and corrupted header rejection.

---

**Author:** KaisarCode

**Email:** <kaisar@kaisarcode.com>

**Website:** [https://kaisarcode.com](https://kaisarcode.com)

**License:** [GNU GPL v3.0](https://www.gnu.org/licenses/gpl-3.0.html)

© 2026 KaisarCode

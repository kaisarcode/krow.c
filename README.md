# krow - Persistent Multi-Value Key-Value Store

A minimalist, high-performance key-value storage engine. It provides persistence for arbitrary data rows mapped to 64-bit keys using memory mapping (mmap).

---

## Quick Start

### Build

Requires a C compiler and CMake 3.14+.

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
```

The `krow` binary is generated directly in the project root.

### Usage

```bash
# Initialize a new store with capacity for 1M records
./krow ini mydb.krow 1000000

# Add records (supports duplicate keys)
./krow set mydb.krow 123 "User profile data"
./krow set mydb.krow 123 "Another record for same key"

# Retrieve records
./krow get mydb.krow 123

# Delete records
./krow del mydb.krow 123

# Defragment heap and rehash index
./krow prune mydb.krow
```

---

## Features

- **Zero-Copy Reads:** data is accessed directly from the kernel page cache via `mmap`.
- **Multi-Value Support:** multiple records per key via open addressing and linear probing.
- **Crash-Safe Writes:** `set`, `del`, and `prune` survive a process crash without corrupting the store.
- **Advisory File Lock:** prevents concurrent writers from sharing a store.
- **Minimalist:** strict POSIX C implementation with zero external dependencies.

---

## Public API

```c
#include "krow.h"

kc_krow_t *ctx = kc_krow_open("data.krow", 1000000);

kc_krow_set(ctx, 0xABC, "Value", 5);
kc_krow_del(ctx, 0xABC);
kc_krow_prune(ctx);
kc_krow_sync(ctx);
kc_krow_close(ctx);
```

---

**Author:** KaisarCode

**Email:** <kaisar@kaisarcode.com>

**Website:** <https://kaisarcode.com>

**License:** <https://www.gnu.org/licenses/gpl-3.0.html>

© 2026 KaisarCode

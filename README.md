# krow - Persistent Multi-Value Key-Value Store

A minimalist, high-performance key-value storage engine. It provides persistence for arbitrary data rows mapped to 64-bit keys using memory mapping (mmap).

---

## Quick Start

### Build
Requires a C compiler and CMake 3.14+.
```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build --config Release
```
*The `krow` binary will be generated directly in the root directory.*

### Usage
```bash
# Initialize a new store with capacity for 1M records
./krow init mydb.krow 1000000

# Add records (supports duplicate keys)
./krow put mydb.krow 123 "User profile data"
./krow put mydb.krow 123 "Another record for same key"

# Retrieve records
./krow get mydb.krow 123
```

---

## Features
- **Zero-Copy**: Data is accessed directly from the kernel page cache via `mmap`.
- **Append-Only Heap**: Prevents corruption and ensures high write speed.
- **Multi-Value Support**: Open addressing with linear probing allows multiple records per key.
- **Minimalist**: Strict POSIX C implementation with zero external dependencies.

---

## Public API
```c
#include "krow.h"

// Open or create store
kc_krow_t *ctx = kc_krow_open("data.krow", 1000000);

// Add data
kc_krow_put(ctx, 0xABC, "Value", 5);

// Sync to disk
kc_krow_sync(ctx);

// Close
kc_krow_close(ctx);
```

---

**Author:** KaisarCode

**Website:** https://kaisarcode.com

**License:** https://www.gnu.org/licenses/gpl-3.0.html

© 2026 KaisarCode

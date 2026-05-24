# PyroDB

A **fast, encrypted, file-based NoSQL database engine** for Python.. zero SQL, minimal syntax, production-ready internals.

```python
from pyro_db import Database

db    = Database("appdata")
users = db.collection("users")

users.create(username="alex", age=18)
users.get(1)
users.filter(age__gte=18).sort("username").limit(10).all()
users.update(1, age=19)
users.delete(1)
```

---

## Table of Contents

1. [Philosophy](#1-philosophy)
2. [Feature Overview](#2-feature-overview)
3. [Installation](#3-installation)
4. [Quick Start](#4-quick-start)
5. [API Reference](#5-api-reference)
   - [Database](#database)
   - [Collection — CRUD](#collection--crud)
   - [Filtering & Querying](#filtering--querying)
   - [Batch Operations](#batch-operations)
   - [Transactions](#transactions)
   - [Schema Validation](#schema-validation)
   - [Encryption](#encryption)
   - [Maintenance](#maintenance)
6. [File Structure](#6-file-structure)
7. [Internal Architecture](#7-internal-architecture)
   - [Storage Engine](#storage-engine)
   - [Write-Ahead Log](#write-ahead-log)
   - [Index Manager](#index-manager)
   - [LRU Cache](#lru-cache)
   - [Locks](#locks)
   - [Encryption Layer](#encryption-layer)
8. [Filter Operators](#8-filter-operators)
9. [Schema Field Types](#9-schema-field-types)
10. [Error Reference](#10-error-reference)
11. [Performance Notes](#11-performance-notes)
12. [Running Tests](#12-running-tests)
13. [Project Layout](#13-project-layout)

---

## 1. Philosophy

PyroDB was designed around one idea: **a database you can understand end-to-end**.

| Goal | How PyroDB achieves it |
|---|---|
| Easy to learn | Pythonic keyword API — no SQL, no DSL |
| Minimal syntax | `create()`, `get()`, `filter()`, `update()`, `delete()` |
| File-based | One directory, plain files — no server, no daemon |
| Fast on large datasets | Append-only writes, hash indexes, LRU cache |
| Safe from corruption | Write-Ahead Log + atomic file writes |
| Optional encryption | AES-256-GCM per line, PBKDF2-SHA256 key derivation |
| Good under heavy use | Thread-safe + cross-process file locks |
| Pythonic | Context managers, fluent query chaining, keyword args |
| Lightweight | Zero required dependencies (encryption needs `cryptography`) |

---

## 2. Feature Overview

- **CRUD** — `create`, `get`, `update`, `delete`
- **Rich querying** — 13 filter operators, sort, limit, offset, pluck
- **Transactions** — atomic commit / rollback with context manager
- **Schema validation** — typed fields, required, defaults, min/max, choices, custom validators
- **Unique indexes** — per-field uniqueness constraints
- **AES-256-GCM encryption** — per-line, with PBKDF2 key derivation
- **LRU cache** — configurable in-memory record cache per collection
- **Write-Ahead Log** — crash-safe; replays on startup automatically
- **File compaction** — reclaim disk space from old record versions
- **Concurrency** — `threading.RLock` + `fcntl`/`msvcrt` file locks
- **Batch helpers** — `create_many`, `update_many`, `delete_many`
- **Zero SQL** — pure Python keyword syntax throughout

---

## 3. Installation

Basic install:

```bash
pip install git+https://github.com/Fidelissimus/pyro_db.git
```

With encryption support:

```bash
pip install "pyro_db[encryption] @ git+https://github.com/Fidelissimus/pyro_db.git"
# or separately:
pip install cryptography
```

Python 3.10+ required.

---

## 4. Quick Start

```python
from pyro_db import Database

# --- Open / create a database ---
db    = Database("myapp_data")          # creates ./myapp_data/ if needed
users = db.collection("users")

# --- Create ---
alice = users.create(username="alice", age=28, city="london")
bob   = users.create(username="bob",   age=25, city="paris")

print(alice)
# {'username': 'alice', 'age': 28, 'city': 'london', 'id': 1}

# --- Get ---
print(users.get(1))                     # by id
print(users.get(username="alice"))      # by field (uses index)

# --- Filter ---
results = users.filter(age__gte=18, city="london").sort("username").all()

# --- Update ---
users.update(1, age=29)

# --- Delete ---
users.delete(2)

# --- Count / Exists ---
print(users.count())                    # 1
print(users.exists(username="alice"))   # True

# --- Close (or use as context manager) ---
db.close()
```

### Context manager form

```python
with Database("myapp_data") as db:
    users = db.collection("users")
    users.create(username="alice", age=28)
```

---

## 5. API Reference

### Database

```python
Database(path, password=None, cache_size=1024, lock_timeout=10.0)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str \| Path` | — | Directory where files are stored. Created if absent. |
| `password` | `str \| None` | `None` | Enables AES-256-GCM encryption. Must match on every open. |
| `cache_size` | `int` | `1024` | LRU cache size per collection (number of records). |
| `lock_timeout` | `float` | `10.0` | Seconds to wait for a file lock. |

```python
db.collection(name, schema=None, unique_fields=None, cache_size=None)
db.list_collections()       # → ['posts', 'users']
db.drop_collection("temp")
db.compact_all()            # compact every collection
db.stats()                  # database-level statistics
db.close()
```

---

### Collection — CRUD

#### `create(**kwargs) → dict`

Insert a new record. Returns the stored record (with `id`).

```python
user = users.create(username="alex", age=18, active=True)
print(user["id"])   # 1
```

#### `get(id=None, **kwargs) → dict`

Fetch one record. Raises `RecordNotFoundError` if absent.

```python
users.get(1)                     # by id (integer positional or keyword)
users.get(id=1)                  # explicit keyword
users.get(username="alex")       # field lookup via index
```

#### `update(id, **kwargs) → dict`

Patch an existing record. Only supplied fields change; all others are preserved.

```python
users.update(1, age=19, city="manchester")
```

#### `delete(id) → None`

Soft-delete. The record is excluded from all future reads immediately.

```python
users.delete(1)
```

---

### Filtering & Querying

`filter(**kwargs)` returns a **`QueryResult`** — a lazy, chainable object.

```python
users.filter(age=18)
users.filter(age__gte=18, active=True)
users.filter(username__startswith="al")
```

#### Chaining

```python
users.filter(active=True) \
     .sort("age", descending=True) \
     .offset(10) \
     .limit(5) \
     .all()
```

#### Terminal methods

| Method | Returns | Description |
|---|---|---|
| `.all()` | `list[dict]` | All matching records |
| `.first()` | `dict \| None` | First matching record or `None` |
| `.count()` | `int` | Number of matches (ignores limit/offset) |
| `.exists()` | `bool` | True if any record matches |
| `.pluck("field")` | `list` | List of values for one field |
| `.pluck("f1", "f2")` | `list[tuple]` | List of tuples for multiple fields |

#### Shorthand helpers on `Collection`

```python
users.all()                              # all live records
users.sort("age")                        # all, sorted ascending
users.sort("age", descending=True)
users.count()
users.exists(city="london")
```

---

### Batch Operations

```python
# Insert many records in one call
users.create_many([
    {"username": "alice", "age": 28},
    {"username": "bob",   "age": 25},
])

# Delete many by id
users.delete_many([1, 2, 3])            # → int (number actually deleted)

# Apply the same patch to multiple records
users.update_many([4, 5, 6], active=False)
```

---

### Transactions

All operations inside the block are buffered and applied atomically on exit. Any exception rolls back the entire batch.

```python
with users.transaction() as txn:
    txn.create(username="alice", age=28)
    txn.update(existing_id, role="admin")
    txn.delete(old_id)
# All three committed atomically ↑

# Rollback on exception:
try:
    with users.transaction() as txn:
        txn.create(username="bob")
        raise RuntimeError("something went wrong")
except RuntimeError:
    pass
# "bob" was never written
```

---

### Schema Validation

```python
from pyro_db.schema import Schema, Field

schema = Schema(
    username = Field(type=str, required=True, min_length=3, max_length=32),
    email    = Field(type=str, required=True),
    age      = Field(type=int, min_value=0, max_value=150, default=None, nullable=True),
    role     = Field(type=str, choices=["admin", "user", "guest"], default="user"),
    password = Field(type=str, required=True, min_length=8,
                     validator=lambda v: None if any(c.isdigit() for c in v)
                                        else "must contain a digit"),
)

users = db.collection("users", schema=schema, unique_fields=["username", "email"])
```

#### `Field` parameters

| Parameter | Type | Description |
|---|---|---|
| `type` | `type \| tuple[type]` | Accepted Python type(s). `None` = any. |
| `required` | `bool` | Must be present on create. |
| `default` | `Any` | Value used when field is absent. Implicitly makes field optional. |
| `min_value` / `max_value` | `float` | Numeric range (inclusive). |
| `min_length` / `max_length` | `int` | String / list length range. |
| `choices` | `Iterable` | Value must be one of these. |
| `validator` | `Callable[[Any], str \| None]` | Custom function; return error string or `None`. |
| `nullable` | `bool` | `None` is accepted even when `type` is set. |

Validation runs on **create** (full) and **update** (partial — required fields not re-checked).

---

### Encryption

```python
# Create an encrypted database
db = Database("secure_data", password="my$ecretP4ss")

# Re-open with the same password
db2 = Database("secure_data", password="my$ecretP4ss")

# Wrong password → EncryptionError or RecordNotFoundError
```

- Encryption uses **AES-256-GCM** (authenticated — tamper detection is built in).
- Keys are derived with **PBKDF2-SHA256** (260 000 iterations).
- A random 16-byte salt is stored in `.salt` on first open.
- Every data line and WAL entry is encrypted **independently**, so partial reads still work.
- The `cryptography` package is required (`pip install cryptography`).

---

### Maintenance

```python
# Rewrite the data file — removes stale versions & deleted records
result = users.compact()
print(result["records_remaining"])  # live records kept
print(result["bytes_saved"])        # bytes reclaimed

# Compact every collection at once
db.compact_all()

# Force index rebuild (after manual file edits or corruption)
users.rebuild_index()

# Permanently delete a collection and all its files
users.drop()
db.drop_collection("old_logs")

# Statistics
users.stats()   # per-collection: record count, file size, cache hit rate
db.stats()      # database-level: all collections, total records and bytes
```

---

## 6. File Structure

```
myapp_data/
│
├── metadata.json          # DB version, encryption flag, collection registry
├── wal.log                # Write-Ahead Log (shared across collections)
├── .salt                  # Encryption salt (only present when encrypted)
├── .db.lock               # Database-level advisory lock
│
├── users.data             # JSONL append-only record store
├── users.index            # Hash index: field → value → [id, ...]
├── users.lock             # Collection-level advisory lock
├── users_id.json          # Auto-increment ID counter
│
├── posts.data
├── posts.index
├── posts.lock
└── posts_id.json
```

### `.data` file

Each line is one record version (latest version wins on reads):

```jsonl
{"_id":1,"_created":1700000000,"_updated":1700000000,"_deleted":false,"username":"alex","age":18}
{"_id":1,"_created":1700000000,"_updated":1700000010,"_deleted":false,"username":"alex","age":19}
{"_id":2,"_created":1700000005,"_updated":1700000005,"_deleted":false,"username":"bob","age":25}
{"_id":2,"_created":1700000005,"_updated":1700000020,"_deleted":true,"username":"bob","age":25}
```

### `.index` file

```json
{
  "username": {"alex": [1], "bob": [2]},
  "age":      {"19": [1], "25": [2]}
}
```

### `wal.log`

```jsonl
{"op":"CREATE","col":"users","id":1,"ts":1700000000,"data":{...}}
{"op":"UPDATE","col":"users","id":1,"ts":1700000010,"data":{...}}
{"op":"CHECKPOINT","col":"users","ts":1700000010}
```

---

## 7. Internal Architecture

```
Database
│
├── WAL (wal.log)                   # shared, crash recovery
│
└── Collection (per name)
    ├── StorageEngine (.data)       # append-only JSONL
    ├── IndexManager  (.index)      # field hash index
    ├── LRUCache      (memory)      # O(1) hot-record reads
    ├── CollectionLock(.lock)       # thread + file lock
    └── _IDCounter    (_id.json)    # monotonic ID generator
```

### Storage Engine

- **Append-only writes** — updates are new lines, not in-place edits.
- **Latest-version semantics** — reading scans the file and returns the last entry per `_id`.
- **Soft delete** — `_deleted: true` records are included in the file but excluded from all queries.
- **Compaction** — rewrites the file keeping only the latest live version of each record.
- Every write goes through `atomic_append` (fsync) for durability.

### Write-Ahead Log

Every mutating operation is written to `wal.log` **before** it hits the `.data` file:

1. `log_create` / `log_update` / `log_delete` → write to WAL.
2. Apply to `.data` and `.index`.
3. `log_checkpoint` → mark WAL entries as safe to discard.

On startup, `Collection._recover()` replays any entries after the last checkpoint.

### Index Manager

- In-memory `dict[field → dict[str(value) → list[id]]]`.
- Updated synchronously on every write.
- Flushed atomically to `.index` after each mutation.
- Loaded from disk on startup (avoiding a full `.data` scan).
- `rebuild()` rescans `.data` and rebuilds from scratch (used after compaction / recovery).

### LRU Cache

- Backed by `collections.OrderedDict` — O(1) put, get, evict.
- Stores **deep copies** — mutations don't affect the cache.
- Invalidated on every write (`invalidate(id)`) and cleared on compaction.
- Configurable `max_size` (default 1 024 records per collection).

### Locks

Two layers:

| Layer | Scope | Mechanism |
|---|---|---|
| `threading.RLock` | In-process threads | Python stdlib |
| `fcntl.flock` (POSIX) / `msvcrt.locking` (Windows) | Cross-process | OS advisory lock |

`CollectionLock.write()` acquires an exclusive lock; `CollectionLock.read()` acquires a shared lock, allowing concurrent reads.

### Encryption Layer

```
password + salt
    │
    └─ PBKDF2-SHA256 (260 000 iters)
         │
         └─ 256-bit key
              │
              └─ AES-256-GCM (random 96-bit nonce per write)
                   │
                   └─ salt(16) | nonce(12) | tag(16) | ciphertext
```

Each data line and WAL entry is an independent ciphertext blob, base64-encoded for line-delimited storage. Any tampering is detected by the GCM authentication tag before decryption returns.

---

## 8. Filter Operators

Append `__operator` to the field name in `filter()` / `QueryResult.filter()`:

| Syntax | Operator | Description |
|---|---|---|
| `field=value` | `eq` | Exact equality (default) |
| `field__eq=value` | `eq` | Explicit equality |
| `field__ne=value` | `ne` | Not equal |
| `field__gt=value` | `gt` | Greater than |
| `field__gte=value` | `gte` | Greater than or equal |
| `field__lt=value` | `lt` | Less than |
| `field__lte=value` | `lte` | Less than or equal |
| `field__in=[v1,v2]` | `in` | Value in list |
| `field__nin=[v1,v2]` | `nin` | Value not in list |
| `field__contains=s` | `contains` | String contains `s` (case-sensitive) |
| `field__icontains=s` | `icontains` | String contains `s` (case-insensitive) |
| `field__startswith=p` | `startswith` | String starts with `p` |
| `field__endswith=s` | `endswith` | String ends with `s` |
| `field__exists=True` | `exists` | Field is present in the record |
| `field__exists=False` | `exists` | Field is absent from the record |

Multiple conditions in one call are ANDed together:

```python
users.filter(age__gte=18, city="london", active=True)
```

---

## 9. Schema Field Types

```python
from pyro_db.schema import Schema, Field

Schema(
    # Required string, 3–32 chars
    username = Field(type=str, required=True, min_length=3, max_length=32),

    # Optional integer, 0–150, default None, accepts None value
    age      = Field(type=int, min_value=0, max_value=150, default=None, nullable=True),

    # Enumerated value with default
    role     = Field(type=str, choices=["admin", "user", "guest"], default="user"),

    # Accept multiple types
    value    = Field(type=(int, float)),

    # Custom validator
    score    = Field(type=int, validator=lambda v: None if 0 <= v <= 100 else "0–100 only"),

    # Any type, no restriction
    metadata = Field(),
)
```

---

## 10. Error Reference

All exceptions inherit from `pyro_db.PyroDB_Error`.

| Exception | Raised when |
|---|---|
| `RecordNotFoundError` | `get()` / `update()` / `delete()` on a non-existent or deleted record |
| `DuplicateIndexError` | A unique field constraint is violated on create or update |
| `SchemaValidationError` | Record data fails schema validation; `.errors` lists all failures |
| `TransactionError` | Committing/rolling back an already-finished transaction |
| `EncryptionError` | Wrong password, corrupt ciphertext, or `cryptography` not installed |
| `CorruptionError` | A data or index file is unreadable and cannot be auto-recovered |
| `LockTimeoutError` | File lock could not be acquired within `lock_timeout` seconds |

```python
from pyro_db.exceptions import RecordNotFoundError, SchemaValidationError

try:
    users.get(999)
except RecordNotFoundError as e:
    print(e.record_id, e.collection)

try:
    users.create(username="x")   # fails schema
except SchemaValidationError as e:
    for err in e.errors:
        print(err)
```

---

## 11. Performance Notes

| Operation | Cost | Notes |
|---|---|---|
| `create` | O(1) append | Index update is O(1) hash insert |
| `get(id)` | O(1) cache hit | Falls back to O(n) file scan on miss |
| `get(field=value)` | O(1) index + O(1) cache | Uses hash index |
| `filter(...)` | O(n) in-memory | Loads all live records, applies predicates |
| `update` | O(n) read + O(1) append | Must read current version first |
| `delete` | O(n) read + O(1) append | Same as update |
| `compact` | O(n) rewrite | Shrinks file; clears cache |

**Tuning tips:**

- Increase `cache_size` for read-heavy workloads (default 1 024).
- Call `compact()` periodically if you perform many updates/deletes.
- Use `unique_fields` to enforce constraints without manual lookups.
- `filter()` loads all live records — for very large collections, compact frequently so fewer lines are scanned.

---

## 12. Running Tests

```bash
# Install dev dependencies
pip install cryptography pytest pytest-cov

# Run all tests
pytest tests/ -v

# With coverage report
pytest tests/ --cov=pyro_db --cov-report=term-missing
```

The test suite covers:

- Utilities (`encode_line`, `decode_line`, `validate_name`, `deep_merge`, …)
- `LRUCache` — eviction, LRU ordering, stats
- `StorageEngine` — append, read, soft-delete, compaction
- `IndexManager` — CRUD sync, unique constraints, flush/reload, rebuild
- `Schema` / `Field` — all constraint types, defaults, custom validators
- `WAL` — logging, checkpoint, compact, truncate
- `QueryResult` — all 14 operators, sort, limit, offset, pluck
- `Collection` — full CRUD, batch ops, transactions, compact, stats
- `Database` — persistence across reopen, collection registry
- **Encryption** — AES-GCM roundtrip, wrong password, mixed mode detection
- **Concurrency** — 20 threads creating records simultaneously, mixed read/write

---

## 13. Project Layout

```
pyro_db/
│
├── __init__.py          # Public API surface
├── core.py              # Database — top-level entry point
├── collection.py        # Collection — CRUD, transactions, compaction
├── storage.py           # StorageEngine — append-only JSONL I/O
├── indexes.py           # IndexManager — hash field indexes
├── query.py             # QueryResult — filter / sort / limit engine
├── wal.py               # WAL — Write-Ahead Log, crash recovery
├── encryption.py        # Encryptor — AES-256-GCM + PBKDF2
├── cache.py             # LRUCache — in-memory record cache
├── locks.py             # CollectionLock / DatabaseLock — file + thread safety
├── schema.py            # Schema / Field — validation descriptors
├── utils.py             # Shared helpers (atomic writes, timestamps, …)
└── exceptions.py        # All custom exception types

tests/
└── test_pyro_db.py      # 102 tests across all modules

examples/
├── basic_usage.py           # CRUD, filter, sort, transaction, batch, compact
├── schema_and_encryption.py # Schema, unique fields, AES encryption
└── concurrency_demo.py      # 20-thread concurrent writes, mixed read/write
```

---

## License

MIT — use freely, commercially or otherwise.

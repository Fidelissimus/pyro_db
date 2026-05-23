"""
examples/concurrency_demo.py
============================
Demonstrates that PyroDB is safe under concurrent access from multiple threads,
and that transactions provide atomic rollback.

Run::

    python examples/concurrency_demo.py
"""

import shutil
import threading
import time
from pathlib import Path

from pyro_db import Database
from pyro_db.exceptions import RecordNotFoundError

DEMO_DB = Path("example_concurrent_db")
if DEMO_DB.exists():
    shutil.rmtree(DEMO_DB)

print("=" * 60)
print("  PyroDB — Concurrency & Transaction Demo")
print("=" * 60)

db = Database(DEMO_DB)
users = db.collection("users")

# ---------------------------------------------------------------------------
# 1. Concurrent creates — all must get unique IDs
# ---------------------------------------------------------------------------
print("\n--- CONCURRENT CREATES (20 threads) ---")

ids = []
errors = []
lock = threading.Lock()


def create_user(thread_id: int):
    try:
        rec = users.create(
            username=f"user_{thread_id:02d}",
            thread=thread_id,
        )
        with lock:
            ids.append(rec["id"])
    except Exception as exc:
        with lock:
            errors.append(exc)


threads = [threading.Thread(target=create_user, args=(i,)) for i in range(20)]
t0 = time.perf_counter()
for t in threads:
    t.start()
for t in threads:
    t.join()
elapsed = time.perf_counter() - t0

print(f"  Threads: 20  |  Duration: {elapsed:.3f}s")
print(f"  Records created: {len(ids)}")
print(f"  Errors: {len(errors)}")
print(f"  IDs are unique: {len(ids) == len(set(ids))}")
assert not errors, f"Unexpected errors: {errors}"
assert len(ids) == len(set(ids)), "Duplicate IDs detected!"

# ---------------------------------------------------------------------------
# 2. Concurrent reads + writes
# ---------------------------------------------------------------------------
print("\n--- CONCURRENT READS + WRITES (mixed) ---")

read_count = [0]
write_count = [0]
read_errors = []
write_errors = []
mixed_lock = threading.Lock()


def reader():
    for _ in range(10):
        try:
            all_recs = users.all()
            with mixed_lock:
                read_count[0] += 1
        except Exception as exc:
            with mixed_lock:
                read_errors.append(exc)


def writer():
    try:
        rec = users.create(username=f"writer_{threading.get_ident()}")
        users.update(rec["id"], flag="updated")
        with mixed_lock:
            write_count[0] += 1
    except Exception as exc:
        with mixed_lock:
            write_errors.append(exc)


mixed_threads = (
    [threading.Thread(target=reader) for _ in range(5)] +
    [threading.Thread(target=writer) for _ in range(5)]
)
for t in mixed_threads:
    t.start()
for t in mixed_threads:
    t.join()

print(f"  Read ops: {read_count[0]}  |  Write ops: {write_count[0]}")
print(f"  Read errors: {len(read_errors)}  |  Write errors: {len(write_errors)}")
assert not read_errors, f"Read errors: {read_errors}"
assert not write_errors, f"Write errors: {write_errors}"

# ---------------------------------------------------------------------------
# 3. Transaction commit
# ---------------------------------------------------------------------------
print("\n--- TRANSACTION COMMIT ---")
before_count = users.count()

with users.transaction() as txn:
    txn.create(username="txn_alice", role="admin")
    txn.create(username="txn_bob",   role="user")
    txn.update(1, flag="touched_by_txn")

after_count = users.count()
print(f"  Records before: {before_count}  |  After commit: {after_count}")
assert after_count == before_count + 2
print(f"  txn_alice: {users.get(username='txn_alice')}")
assert users.get(1)["flag"] == "touched_by_txn"
print("  ✓ Transaction committed successfully.")

# ---------------------------------------------------------------------------
# 4. Transaction rollback on exception
# ---------------------------------------------------------------------------
print("\n--- TRANSACTION ROLLBACK ---")
before_rollback = users.count()

try:
    with users.transaction() as txn:
        txn.create(username="should_not_exist")
        raise ValueError("Simulated failure — trigger rollback")
except ValueError:
    pass

after_rollback = users.count()
print(f"  Records before: {before_rollback}  |  After (failed) txn: {after_rollback}")
assert after_rollback == before_rollback, "Rollback failed — record was committed!"

try:
    users.get(username="should_not_exist")
    print("  ERROR: record should not exist after rollback")
except RecordNotFoundError:
    print("  ✓ Rolled-back record correctly absent.")

# ---------------------------------------------------------------------------
# 5. Compact and final stats
# ---------------------------------------------------------------------------
print("\n--- COMPACT + STATS ---")
compact_result = users.compact()
print(f"  Records remaining: {compact_result['records_remaining']}")
print(f"  Bytes saved:       {compact_result['bytes_saved']}")

import json
print("\n  Database stats:")
print(json.dumps(db.stats(), indent=4))

db.close()
print("\nDone.")

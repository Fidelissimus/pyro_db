"""
examples/basic_usage.py
=======================
Basic PyroDB usage — CRUD, filtering, sorting, and transactions.

Run::

    python examples/basic_usage.py
"""

from pyro_db import Database
from pyro_db.exceptions import RecordNotFoundError

print("=" * 60)
print("  PyroDB — Basic Usage Example")
print("=" * 60)

# ---------------------------------------------------------------------------
# 1. Open (or create) a database
# ---------------------------------------------------------------------------
db = Database("example_db")
users = db.collection("users")

# Drop any records from a previous run.
for rec in users.all():
    users.delete(rec["id"])

print("\n--- CREATE ---")
alex   = users.create(username="alex",   age=18, city="london",  active=True)
bob    = users.create(username="bob",    age=25, city="paris",   active=True)
carol  = users.create(username="carol",  age=30, city="london",  active=False)
dave   = users.create(username="dave",   age=17, city="berlin",  active=True)
eve    = users.create(username="eve",    age=22, city="london",  active=True)

for rec in [alex, bob, carol, dave, eve]:
    print(f"  Created: {rec}")

# ---------------------------------------------------------------------------
# 2. GET by id and by field
# ---------------------------------------------------------------------------
print("\n--- GET ---")
print(f"  get(id=1) → {users.get(1)}")
print(f"  get(username='bob') → {users.get(username='bob')}")

# ---------------------------------------------------------------------------
# 3. UPDATE
# ---------------------------------------------------------------------------
print("\n--- UPDATE ---")
updated = users.update(alex["id"], age=19, city="manchester")
print(f"  update(1, age=19, city='manchester') → {updated}")

# ---------------------------------------------------------------------------
# 4. FILTER with operators
# ---------------------------------------------------------------------------
print("\n--- FILTER ---")

adults = users.filter(age__gte=18).all()
print(f"  age >= 18 ({len(adults)} records):")
for r in adults:
    print(f"    {r}")

london_active = users.filter(city="london", active=True).all()
print(f"\n  city='london' AND active=True ({len(london_active)} records):")
for r in london_active:
    print(f"    {r}")

# ---------------------------------------------------------------------------
# 5. SORT and LIMIT
# ---------------------------------------------------------------------------
print("\n--- SORT + LIMIT ---")
top3 = users.sort("age", descending=True).limit(3).all()
print("  Oldest 3 users:")
for r in top3:
    print(f"    {r['username']} — age {r['age']}")

# ---------------------------------------------------------------------------
# 6. COUNT and EXISTS
# ---------------------------------------------------------------------------
print("\n--- COUNT / EXISTS ---")
print(f"  Total users: {users.count()}")
print(f"  Any user in 'tokyo'? {users.exists(city='tokyo')}")
print(f"  Any user in 'london'? {users.exists(city='london')}")

# ---------------------------------------------------------------------------
# 7. PLUCK — extract a single field
# ---------------------------------------------------------------------------
print("\n--- PLUCK ---")
names = users.filter(active=True).sort("username").pluck("username")
print(f"  Active usernames (sorted): {names}")

# ---------------------------------------------------------------------------
# 8. DELETE
# ---------------------------------------------------------------------------
print("\n--- DELETE ---")
users.delete(dave["id"])
print(f"  Deleted dave (id={dave['id']})")
try:
    users.get(dave["id"])
except RecordNotFoundError:
    print("  Confirmed: dave is gone.")

# ---------------------------------------------------------------------------
# 9. TRANSACTION — atomic batch of writes
# ---------------------------------------------------------------------------
print("\n--- TRANSACTION ---")
with users.transaction() as txn:
    txn.create(username="frank", age=28, city="rome", active=True)
    txn.update(bob["id"], city="amsterdam")

print("  Transaction committed: frank created, bob moved to amsterdam.")
print(f"  Frank: {users.get(username='frank')}")
print(f"  Bob: {users.get(bob['id'])}")

# ---------------------------------------------------------------------------
# 10. BATCH operations
# ---------------------------------------------------------------------------
print("\n--- BATCH ---")
batch = users.create_many([
    {"username": "grace", "age": 29, "city": "tokyo",  "active": True},
    {"username": "hank",  "age": 33, "city": "sydney", "active": False},
])
print(f"  Created {len(batch)} records in one call.")

ids_to_deactivate = [r["id"] for r in users.filter(active=False).all()]
users.update_many(ids_to_deactivate, active=False)
print(f"  Deactivated {len(ids_to_deactivate)} users.")

# ---------------------------------------------------------------------------
# 11. COMPACT — reclaim disk space
# ---------------------------------------------------------------------------
print("\n--- COMPACT ---")
result = users.compact()
print(f"  Compaction: {result['records_remaining']} records, "
      f"{result['bytes_saved']} bytes reclaimed.")

# ---------------------------------------------------------------------------
# 12. STATS
# ---------------------------------------------------------------------------
print("\n--- STATS ---")
import json
s = users.stats()
print(json.dumps(s, indent=2))

# ---------------------------------------------------------------------------
# 13. ALL at the end
# ---------------------------------------------------------------------------
print("\n--- FINAL STATE (all users) ---")
for r in users.sort("username").all():
    print(f"  {r}")

db.close()
print("\nDone. Database files are in ./example_db/")

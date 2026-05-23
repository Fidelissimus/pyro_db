"""
examples/schema_and_encryption.py
==================================
Demonstrates schema validation, unique field constraints, and AES-256-GCM
encryption.

Run::

    pip install cryptography
    python examples/schema_and_encryption.py
"""

import json
import shutil
from pathlib import Path

from pyro_db import Database
from pyro_db.exceptions import DuplicateIndexError, SchemaValidationError
from pyro_db.schema import Field, Schema

# Clean up from previous run
ENC_DB_PATH = Path("example_enc_db")
if ENC_DB_PATH.exists():
    shutil.rmtree(ENC_DB_PATH)

print("=" * 60)
print("  PyroDB — Schema + Encryption Example")
print("=" * 60)

# ---------------------------------------------------------------------------
# 1. Define a schema
# ---------------------------------------------------------------------------
print("\n--- SCHEMA DEFINITION ---")

def strong_password(v: str):
    """Custom validator: password must contain at least one digit."""
    if not any(c.isdigit() for c in v):
        return "password must contain at least one digit"
    return None

user_schema = Schema(
    username=Field(
        type=str,
        required=True,
        min_length=3,
        max_length=32,
    ),
    email=Field(
        type=str,
        required=True,
    ),
    age=Field(
        type=int,
        required=False,
        min_value=0,
        max_value=150,
        default=None,
        nullable=True,
    ),
    role=Field(
        type=str,
        choices=["admin", "user", "guest"],
        default="user",
    ),
    password=Field(
        type=str,
        required=True,
        min_length=8,
        validator=strong_password,
    ),
)

print(f"  Schema defined: {user_schema}")

# ---------------------------------------------------------------------------
# 2. Open an encrypted database with the schema
# ---------------------------------------------------------------------------
print("\n--- ENCRYPTED DATABASE ---")
PASSWORD = "my$ecretP4ss"
db = Database(ENC_DB_PATH, password=PASSWORD)
users = db.collection(
    "users",
    schema=user_schema,
    unique_fields=["username", "email"],
)
print(f"  Opened: {db}")

# ---------------------------------------------------------------------------
# 3. Create valid records
# ---------------------------------------------------------------------------
print("\n--- CREATE VALID RECORDS ---")
alice = users.create(
    username="alice",
    email="alice@example.com",
    age=28,
    role="admin",
    password="Alicepass1",
)
bob = users.create(
    username="bob",
    email="bob@example.com",
    role="user",  # age omitted → default None
    password="Bobpass99",
)
print(f"  alice: {alice}")
print(f"  bob:   {bob}")
print(f"  bob['role'] (default applied): {bob['role']}")
print(f"  bob['age']  (default applied): {bob['age']}")

# ---------------------------------------------------------------------------
# 4. Schema validation failures
# ---------------------------------------------------------------------------
print("\n--- SCHEMA VALIDATION FAILURES ---")

# Missing required field
try:
    users.create(username="x", password="noatall1")
except SchemaValidationError as e:
    print(f"  ✓ Missing email caught: {e}")

# Username too short
try:
    users.create(username="ab", email="ab@x.com", password="pass1234")
except SchemaValidationError as e:
    print(f"  ✓ Short username caught: {e}")

# Invalid role
try:
    users.create(username="test_user", email="t@x.com", password="pass1234", role="superuser")
except SchemaValidationError as e:
    print(f"  ✓ Invalid role caught: {e}")

# Password without digit (custom validator)
try:
    users.create(username="carol", email="carol@x.com", password="nodigitshere")
except SchemaValidationError as e:
    print(f"  ✓ Weak password caught: {e}")

# Age out of range
try:
    users.create(username="ghost", email="ghost@x.com", password="pass1234", age=200)
except SchemaValidationError as e:
    print(f"  ✓ Age out of range caught: {e}")

# ---------------------------------------------------------------------------
# 5. Unique constraint violations
# ---------------------------------------------------------------------------
print("\n--- UNIQUE CONSTRAINT VIOLATIONS ---")

try:
    users.create(
        username="alice",           # duplicate
        email="alice2@example.com",
        password="AnotherP4ss",
    )
except DuplicateIndexError as e:
    print(f"  ✓ Duplicate username caught: {e}")

try:
    users.create(
        username="alice2",
        email="alice@example.com",  # duplicate
        password="AnotherP4ss",
    )
except DuplicateIndexError as e:
    print(f"  ✓ Duplicate email caught: {e}")

# ---------------------------------------------------------------------------
# 6. Partial update respects schema
# ---------------------------------------------------------------------------
print("\n--- PARTIAL UPDATE ---")
updated = users.update(alice["id"], age=29)
print(f"  alice after age update: {updated}")

try:
    users.update(alice["id"], role="overlord")
except SchemaValidationError as e:
    print(f"  ✓ Invalid role in update caught: {e}")

# ---------------------------------------------------------------------------
# 7. Close and reopen — data is still there and encrypted
# ---------------------------------------------------------------------------
print("\n--- PERSISTENCE ACROSS REOPEN ---")
db.close()

db2 = Database(ENC_DB_PATH, password=PASSWORD)
users2 = db2.collection("users", schema=user_schema, unique_fields=["username", "email"])
all_users = users2.all()
print(f"  Records after reopen: {len(all_users)}")
for r in all_users:
    print(f"    {r}")

# ---------------------------------------------------------------------------
# 8. Wrong password attempt
# ---------------------------------------------------------------------------
print("\n--- WRONG PASSWORD ---")
db2.close()

from pyro_db.exceptions import EncryptionError
try:
    db_bad = Database(ENC_DB_PATH, password="wrongpassword")
    users_bad = db_bad.collection("users")
    users_bad.get(alice["id"])
    db_bad.close()
    print("  WARNING: expected failure did not occur")
except Exception as e:
    print(f"  ✓ Wrong password correctly rejected: {type(e).__name__}")

print("\nDone.")

"""
tests/test_pyro_db.py
=====================
Comprehensive test suite for PyroDB.

Run with::

    pytest tests/ -v
"""

from __future__ import annotations

import os
import tempfile
import threading
import time
from pathlib import Path

import pytest

from pyro_db import Database
from pyro_db.cache import LRUCache
from pyro_db.exceptions import (
    DuplicateIndexError,
    LockTimeoutError,
    RecordNotFoundError,
    SchemaValidationError,
    TransactionError,
)
from pyro_db.indexes import IndexManager
from pyro_db.query import QueryResult, _parse_kwargs
from pyro_db.schema import Field, Schema
from pyro_db.storage import StorageEngine
from pyro_db.utils import (
    clamp,
    decode_line,
    deep_merge,
    encode_line,
    now_ts,
    strip_internal_keys,
    validate_name,
)
from pyro_db.wal import WAL


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_db_path(tmp_path):
    """Return a fresh temp directory for each test."""
    return tmp_path / "testdb"


@pytest.fixture
def db(tmp_db_path):
    """Return a fresh Database instance for each test."""
    with Database(tmp_db_path) as database:
        yield database


@pytest.fixture
def users(db):
    """Return a 'users' Collection on the test database."""
    return db.collection("users")


# ===========================================================================
# utils tests
# ===========================================================================

class TestUtils:
    def test_now_ts_returns_int(self):
        ts = now_ts()
        assert isinstance(ts, int)
        assert ts > 0

    def test_encode_decode_roundtrip(self):
        record = {"_id": 1, "name": "alex", "age": 18}
        encoded = encode_line(record)
        assert encoded.endswith(b"\n")
        decoded = decode_line(encoded)
        assert decoded == record

    def test_decode_blank_line_returns_none(self):
        assert decode_line(b"") is None
        assert decode_line(b"   \n") is None

    def test_validate_name_valid(self):
        assert validate_name("users") == "users"
        assert validate_name("_internal") == "_internal"
        assert validate_name("my_col_2") == "my_col_2"

    def test_validate_name_invalid(self):
        with pytest.raises(ValueError):
            validate_name("2start")
        with pytest.raises(ValueError):
            validate_name("has space")
        with pytest.raises(ValueError):
            validate_name("")
        with pytest.raises(ValueError):
            validate_name("a" * 65)

    def test_deep_merge_flat(self):
        base = {"a": 1, "b": 2}
        updates = {"b": 99, "c": 3}
        result = deep_merge(base, updates)
        assert result == {"a": 1, "b": 99, "c": 3}
        assert base == {"a": 1, "b": 2}  # not mutated

    def test_deep_merge_nested(self):
        base = {"a": {"x": 1, "y": 2}}
        updates = {"a": {"y": 99, "z": 3}}
        result = deep_merge(base, updates)
        assert result["a"] == {"x": 1, "y": 99, "z": 3}

    def test_clamp(self):
        assert clamp(5, 0, 10) == 5
        assert clamp(-1, 0, 10) == 0
        assert clamp(99, 0, 10) == 10

    def test_strip_internal_keys(self):
        record = {"_id": 1, "_created": 123, "name": "alex"}
        result = strip_internal_keys(record)
        assert result == {"name": "alex"}


# ===========================================================================
# LRU Cache tests
# ===========================================================================

class TestLRUCache:
    def test_miss_returns_none(self):
        cache = LRUCache(max_size=10)
        assert cache.get(999) is None

    def test_put_and_get(self):
        cache = LRUCache(max_size=10)
        record = {"_id": 1, "name": "alex"}
        cache.put(1, record)
        result = cache.get(1)
        assert result == record

    def test_put_returns_copy(self):
        cache = LRUCache(max_size=10)
        original = {"_id": 1, "name": "alex"}
        cache.put(1, original)
        original["name"] = "mutated"
        assert cache.get(1)["name"] == "alex"

    def test_eviction_at_capacity(self):
        cache = LRUCache(max_size=3)
        for i in range(4):
            cache.put(i, {"_id": i})
        assert cache.get(0) is None  # evicted
        assert cache.get(1) is not None

    def test_lru_order_maintained(self):
        cache = LRUCache(max_size=3)
        for i in range(3):
            cache.put(i, {"_id": i})
        # Access id=0 to make it recently used.
        cache.get(0)
        # Add a fourth item — id=1 should be evicted (LRU).
        cache.put(3, {"_id": 3})
        assert cache.get(0) is not None
        assert cache.get(1) is None  # LRU evicted
        assert cache.get(2) is not None

    def test_invalidate(self):
        cache = LRUCache(max_size=10)
        cache.put(1, {"_id": 1})
        cache.invalidate(1)
        assert cache.get(1) is None

    def test_clear(self):
        cache = LRUCache(max_size=10)
        for i in range(5):
            cache.put(i, {"_id": i})
        cache.clear()
        assert cache.size == 0

    def test_stats(self):
        cache = LRUCache(max_size=10)
        cache.put(1, {"_id": 1})
        cache.get(1)   # hit
        cache.get(99)  # miss
        s = cache.stats()
        assert s["hits"] == 1
        assert s["misses"] == 1
        assert s["hit_rate"] == 0.5

    def test_invalid_max_size_raises(self):
        with pytest.raises(ValueError):
            LRUCache(max_size=0)


# ===========================================================================
# StorageEngine tests
# ===========================================================================

class TestStorageEngine:
    def test_append_and_read(self, tmp_path):
        engine = StorageEngine(tmp_path / "col.data")
        record = {"_id": 1, "_deleted": False, "name": "alex"}
        engine.append_record(record)
        result = engine.read_record(1)
        assert result is not None
        assert result["name"] == "alex"

    def test_read_nonexistent_returns_none(self, tmp_path):
        engine = StorageEngine(tmp_path / "col.data")
        assert engine.read_record(999) is None

    def test_soft_delete_excluded(self, tmp_path):
        engine = StorageEngine(tmp_path / "col.data")
        engine.append_record({"_id": 1, "_deleted": False, "name": "alex"})
        engine.append_record({"_id": 1, "_deleted": True, "name": "alex"})
        assert engine.read_record(1) is None

    def test_latest_version_wins(self, tmp_path):
        engine = StorageEngine(tmp_path / "col.data")
        engine.append_record({"_id": 1, "_deleted": False, "name": "old"})
        engine.append_record({"_id": 1, "_deleted": False, "name": "new"})
        result = engine.read_record(1)
        assert result["name"] == "new"

    def test_load_live_filters_deleted(self, tmp_path):
        engine = StorageEngine(tmp_path / "col.data")
        engine.append_record({"_id": 1, "_deleted": False, "name": "alex"})
        engine.append_record({"_id": 2, "_deleted": False, "name": "bob"})
        engine.append_record({"_id": 2, "_deleted": True, "name": "bob"})
        live = engine.load_live()
        assert 1 in live
        assert 2 not in live

    def test_compact_shrinks_file(self, tmp_path):
        engine = StorageEngine(tmp_path / "col.data")
        for i in range(5):
            engine.append_record({"_id": 1, "_deleted": False, "val": i})
        size_before = engine.file_size()
        engine.compact()
        assert engine.file_size() < size_before
        assert engine.read_record(1)["val"] == 4  # latest version


# ===========================================================================
# IndexManager tests
# ===========================================================================

class TestIndexManager:
    def test_lookup_after_create(self, tmp_path):
        idx = IndexManager(tmp_path / "col.index")
        record = {"_id": 1, "_deleted": False, "username": "alex", "age": 18}
        idx.on_create(record)
        assert idx.lookup("username", "alex") == [1]
        assert idx.lookup("age", 18) == [1]

    def test_lookup_after_delete(self, tmp_path):
        idx = IndexManager(tmp_path / "col.index")
        record = {"_id": 1, "_deleted": False, "username": "alex"}
        idx.on_create(record)
        idx.on_delete(record)
        assert idx.lookup("username", "alex") == []

    def test_unique_constraint_violation(self, tmp_path):
        idx = IndexManager(tmp_path / "col.index", unique_fields=["username"])
        idx.on_create({"_id": 1, "username": "alex"})
        with pytest.raises(DuplicateIndexError):
            idx.on_create({"_id": 2, "username": "alex"})

    def test_update_replaces_old_values(self, tmp_path):
        idx = IndexManager(tmp_path / "col.index")
        old = {"_id": 1, "age": 18}
        idx.on_create(old)
        new = {"_id": 1, "age": 19}
        idx.on_update(old, new)
        assert idx.lookup("age", 18) == []
        assert idx.lookup("age", 19) == [1]

    def test_flush_and_reload(self, tmp_path):
        path = tmp_path / "col.index"
        idx = IndexManager(path)
        idx.on_create({"_id": 1, "name": "alex"})
        idx.flush()
        idx2 = IndexManager(path)
        assert idx2.lookup("name", "alex") == [1]

    def test_rebuild(self, tmp_path):
        idx = IndexManager(tmp_path / "col.index")
        records = {
            1: {"_id": 1, "city": "london"},
            2: {"_id": 2, "city": "paris"},
        }
        idx.rebuild(records)
        assert idx.lookup("city", "london") == [1]
        assert idx.lookup("city", "paris") == [2]


# ===========================================================================
# Schema tests
# ===========================================================================

class TestSchema:
    def test_valid_record_passes(self):
        schema = Schema(
            username=Field(type=str, required=True, min_length=3),
            age=Field(type=int, min_value=0, max_value=150),
        )
        schema.validate({"username": "alex", "age": 18})  # should not raise

    def test_missing_required_field_raises(self):
        schema = Schema(username=Field(type=str, required=True))
        with pytest.raises(SchemaValidationError) as exc_info:
            schema.validate({})
        assert "username" in str(exc_info.value)

    def test_wrong_type_raises(self):
        schema = Schema(age=Field(type=int))
        with pytest.raises(SchemaValidationError):
            schema.validate({"age": "not_an_int"})

    def test_min_value_violation_raises(self):
        schema = Schema(age=Field(type=int, min_value=0))
        with pytest.raises(SchemaValidationError):
            schema.validate({"age": -1})

    def test_choices_violation_raises(self):
        schema = Schema(role=Field(type=str, choices=["admin", "user"]))
        with pytest.raises(SchemaValidationError):
            schema.validate({"role": "superuser"})

    def test_defaults_applied(self):
        schema = Schema(role=Field(type=str, default="user"))
        result = schema.apply_defaults({})
        assert result["role"] == "user"

    def test_partial_validation_skips_required(self):
        schema = Schema(username=Field(type=str, required=True))
        # Partial updates should not complain about missing required fields.
        schema.validate({}, partial=True)  # should not raise

    def test_custom_validator(self):
        def must_be_even(v):
            return None if v % 2 == 0 else "must be even"
        schema = Schema(num=Field(type=int, validator=must_be_even))
        with pytest.raises(SchemaValidationError):
            schema.validate({"num": 3})
        schema.validate({"num": 4})  # should pass


# ===========================================================================
# WAL tests
# ===========================================================================

class TestWAL:
    def test_log_and_recover_create(self, tmp_path):
        wal = WAL(tmp_path)
        record = {"_id": 1, "_created": 0, "_updated": 0, "_deleted": False, "name": "alex"}
        wal.log_create("users", record)
        pending = wal.pending_entries("users")
        assert len(pending) == 1
        assert pending[0]["op"] == "CREATE"

    def test_checkpoint_clears_pending(self, tmp_path):
        wal = WAL(tmp_path)
        record = {"_id": 1, "_created": 0, "_updated": 0, "_deleted": False, "name": "alex"}
        wal.log_create("users", record)
        wal.log_checkpoint("users")
        assert wal.pending_entries("users") == []

    def test_compact_removes_checkpointed(self, tmp_path):
        wal = WAL(tmp_path)
        for i in range(5):
            wal.log_create("users", {"_id": i, "_created": 0, "_updated": 0, "_deleted": False})
        wal.log_checkpoint("users")
        wal.compact()
        assert wal.pending_entries("users") == []

    def test_truncate_removes_file(self, tmp_path):
        wal = WAL(tmp_path)
        wal.log_create("users", {"_id": 1, "_created": 0, "_updated": 0, "_deleted": False})
        wal.truncate()
        assert not wal.path.exists()


# ===========================================================================
# QueryResult tests
# ===========================================================================

class TestQueryResult:
    RECORDS = [
        {"_id": 1, "name": "alice", "age": 30, "city": "london"},
        {"_id": 2, "name": "bob",   "age": 25, "city": "paris"},
        {"_id": 3, "name": "carol", "age": 30, "city": "london"},
        {"_id": 4, "name": "dave",  "age": 17, "city": "berlin"},
    ]

    def qr(self, strip=False):
        return QueryResult(list(self.RECORDS), strip_meta=strip)

    def test_all_returns_all(self):
        assert len(self.qr().all()) == 4

    def test_filter_eq(self):
        result = self.qr().filter(age=30).all()
        assert len(result) == 2

    def test_filter_gte(self):
        result = self.qr().filter(age__gte=25).all()
        assert len(result) == 3

    def test_filter_lt(self):
        result = self.qr().filter(age__lt=25).all()
        assert len(result) == 1

    def test_filter_in(self):
        result = self.qr().filter(city__in=["london", "berlin"]).all()
        assert len(result) == 3

    def test_filter_contains(self):
        result = self.qr().filter(name__contains="ob").all()
        assert len(result) == 1
        assert result[0]["name"] == "bob"

    def test_filter_icontains(self):
        result = self.qr().filter(name__icontains="OB").all()
        assert len(result) == 1

    def test_filter_startswith(self):
        result = self.qr().filter(name__startswith="a").all()
        assert len(result) == 1

    def test_filter_exists_true(self):
        result = self.qr().filter(city__exists=True).all()
        assert len(result) == 4

    def test_filter_exists_false(self):
        result = self.qr().filter(nonexistent__exists=False).all()
        assert len(result) == 4

    def test_sort_ascending(self):
        result = self.qr().sort("age").all()
        ages = [r["age"] for r in result]
        assert ages == sorted(ages)

    def test_sort_descending(self):
        result = self.qr().sort("age", descending=True).all()
        ages = [r["age"] for r in result]
        assert ages == sorted(ages, reverse=True)

    def test_limit(self):
        result = self.qr().limit(2).all()
        assert len(result) == 2

    def test_offset(self):
        result = self.qr().offset(2).all()
        assert len(result) == 2

    def test_first_returns_single(self):
        result = self.qr().filter(name="bob").first()
        assert result["name"] == "bob"

    def test_first_returns_none_on_empty(self):
        assert self.qr().filter(name="nobody").first() is None

    def test_count(self):
        assert self.qr().filter(city="london").count() == 2

    def test_exists_true(self):
        assert self.qr().filter(name="alice").exists()

    def test_exists_false(self):
        assert not self.qr().filter(name="nobody").exists()

    def test_pluck_single_field(self):
        names = self.qr().sort("name").pluck("name")
        assert names == ["alice", "bob", "carol", "dave"]

    def test_pluck_multiple_fields(self):
        pairs = self.qr().filter(age=30).sort("name").pluck("name", "city")
        assert ("alice", "london") in pairs

    def test_unknown_operator_raises(self):
        with pytest.raises(ValueError):
            self.qr().filter(name__badop="x").all()

    def test_chaining(self):
        result = (
            self.qr()
            .filter(age__gte=18)
            .sort("name")
            .limit(2)
            .all()
        )
        assert len(result) == 2


# ===========================================================================
# Collection / Database integration tests
# ===========================================================================

class TestCollectionCRUD:
    def test_create_returns_record_with_id(self, users):
        rec = users.create(username="alex", age=18)
        assert "id" in rec
        assert rec["id"] == 1
        assert rec["username"] == "alex"

    def test_get_by_id(self, users):
        users.create(username="alex", age=18)
        rec = users.get(1)
        assert rec["username"] == "alex"

    def test_get_by_field(self, users):
        users.create(username="alex", age=18)
        rec = users.get(username="alex")
        assert rec["id"] == 1

    def test_get_nonexistent_raises(self, users):
        with pytest.raises(RecordNotFoundError):
            users.get(999)

    def test_update_changes_fields(self, users):
        users.create(username="alex", age=18)
        updated = users.update(1, age=19)
        assert updated["age"] == 19
        assert updated["username"] == "alex"

    def test_update_nonexistent_raises(self, users):
        with pytest.raises(RecordNotFoundError):
            users.update(999, age=20)

    def test_delete_removes_from_get(self, users):
        users.create(username="alex", age=18)
        users.delete(1)
        with pytest.raises(RecordNotFoundError):
            users.get(1)

    def test_delete_nonexistent_raises(self, users):
        with pytest.raises(RecordNotFoundError):
            users.delete(999)

    def test_all_returns_live_records(self, users):
        users.create(username="alex")
        users.create(username="bob")
        users.delete(1)
        all_recs = users.all()
        assert len(all_recs) == 1
        assert all_recs[0]["username"] == "bob"

    def test_count(self, users):
        users.create(username="alex")
        users.create(username="bob")
        assert users.count() == 2

    def test_filter_chain(self, users):
        users.create(username="alex", age=18)
        users.create(username="bob", age=25)
        users.create(username="carol", age=18)
        result = users.filter(age=18).sort("username").all()
        assert [r["username"] for r in result] == ["alex", "carol"]

    def test_exists(self, users):
        users.create(username="alex")
        assert users.exists(username="alex")
        assert not users.exists(username="nobody")

    def test_sort(self, users):
        users.create(username="charlie")
        users.create(username="alice")
        users.create(username="bob")
        result = users.sort("username").all()
        names = [r["username"] for r in result]
        assert names == sorted(names)

    def test_create_many(self, users):
        records = [{"username": f"user{i}"} for i in range(5)]
        created = users.create_many(records)
        assert len(created) == 5
        assert users.count() == 5

    def test_delete_many(self, users):
        for i in range(5):
            users.create(username=f"user{i}")
        deleted = users.delete_many([1, 2, 3])
        assert deleted == 3
        assert users.count() == 2

    def test_update_many(self, users):
        for i in range(3):
            users.create(username=f"user{i}", active=False)
        updated = users.update_many([1, 2], active=True)
        assert updated == 2
        assert users.get(1)["active"] is True
        assert users.get(3)["active"] is False

    def test_compact(self, users):
        users.create(username="alex")
        users.update(1, username="ALEX")
        users.update(1, username="Alex")
        result = users.compact()
        assert result["records_remaining"] == 1

    def test_stats(self, users):
        users.create(username="alex")
        s = users.stats()
        assert s["name"] == "users"
        assert s["record_count"] == 1
        assert "cache" in s


class TestTransaction:
    def test_commit_applies_all_ops(self, users):
        with users.transaction() as txn:
            txn.create(username="alex", age=18)
            txn.create(username="bob", age=25)
        assert users.count() == 2

    def test_rollback_on_exception(self, users):
        try:
            with users.transaction() as txn:
                txn.create(username="alex")
                raise RuntimeError("boom")
        except RuntimeError:
            pass
        assert users.count() == 0

    def test_nested_ops_in_transaction(self, users):
        users.create(username="existing")
        with users.transaction() as txn:
            txn.update(1, username="updated")
        assert users.get(1)["username"] == "updated"


class TestSchema_Collection:
    def test_schema_validation_on_create(self, db):
        schema = Schema(
            username=Field(type=str, required=True, min_length=3),
            age=Field(type=int, min_value=0),
        )
        users = db.collection("users", schema=schema)
        with pytest.raises(SchemaValidationError):
            users.create(username="ab")  # too short

    def test_schema_allows_valid(self, db):
        schema = Schema(username=Field(type=str, required=True))
        users = db.collection("users", schema=schema)
        rec = users.create(username="alex")
        assert rec["username"] == "alex"

    def test_unique_constraint(self, db):
        users = db.collection("users", unique_fields=["email"])
        users.create(email="alex@example.com")
        with pytest.raises(DuplicateIndexError):
            users.create(email="alex@example.com")


class TestPersistence:
    def test_data_survives_reopen(self, tmp_db_path):
        with Database(tmp_db_path) as db:
            users = db.collection("users")
            users.create(username="alex", age=18)

        with Database(tmp_db_path) as db2:
            users2 = db2.collection("users")
            rec = users2.get(1)
            assert rec["username"] == "alex"

    def test_delete_survives_reopen(self, tmp_db_path):
        with Database(tmp_db_path) as db:
            users = db.collection("users")
            users.create(username="alex")
            users.delete(1)

        with Database(tmp_db_path) as db2:
            users2 = db2.collection("users")
            with pytest.raises(RecordNotFoundError):
                users2.get(1)

    def test_collection_registry_persisted(self, tmp_db_path):
        with Database(tmp_db_path) as db:
            db.collection("users")
            db.collection("posts")

        with Database(tmp_db_path) as db2:
            assert "users" in db2.list_collections()
            assert "posts" in db2.list_collections()


class TestEncryption:
    def test_encrypted_data_readable(self, tmp_db_path):
        with Database(tmp_db_path, password="secret") as db:
            users = db.collection("users")
            users.create(username="alex", age=18)

        with Database(tmp_db_path, password="secret") as db2:
            users2 = db2.collection("users")
            rec = users2.get(1)
            assert rec["username"] == "alex"

    def test_wrong_password_raises(self, tmp_db_path):
        with Database(tmp_db_path, password="correct") as db:
            db.collection("users").create(username="alex")

        # Opening with wrong password fails during reads (decryption error),
        # or at least must not silently return wrong data.
        from pyro_db.exceptions import EncryptionError
        with Database(tmp_db_path, password="wrong") as db2:
            users2 = db2.collection("users")
            with pytest.raises(Exception):  # EncryptionError or CorruptionError
                users2.get(1)

    def test_open_encrypted_without_password_raises(self, tmp_db_path):
        from pyro_db.exceptions import EncryptionError
        with Database(tmp_db_path, password="secret") as db:
            db.collection("users").create(username="alex")

        with pytest.raises(EncryptionError):
            Database(tmp_db_path)  # no password


class TestConcurrency:
    def test_concurrent_creates_unique_ids(self, users):
        ids = []
        errors = []
        lock = threading.Lock()

        def worker():
            try:
                rec = users.create(username=f"user_{threading.get_ident()}")
                with lock:
                    ids.append(rec["id"])
            except Exception as exc:
                with lock:
                    errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Errors during concurrent writes: {errors}"
        assert len(ids) == len(set(ids)), "Duplicate IDs detected"
        assert len(ids) == 20


class TestDatabaseLevel:
    def test_list_collections(self, db):
        db.collection("alpha")
        db.collection("beta")
        assert "alpha" in db.list_collections()
        assert "beta" in db.list_collections()

    def test_drop_collection(self, db):
        db.collection("temp")
        db.drop_collection("temp")
        assert "temp" not in db.list_collections()

    def test_compact_all(self, db):
        users = db.collection("users")
        users.create(username="alex")
        users.update(1, username="ALEX")
        results = db.compact_all()
        assert "users" in results

    def test_stats(self, db):
        db.collection("users").create(username="alex")
        s = db.stats()
        assert s["total_records"] == 1
        assert "encrypted" in s

    def test_context_manager(self, tmp_db_path):
        with Database(tmp_db_path) as db:
            db.collection("users").create(username="alex")
        # After close, collections dict is empty.
        assert not db._collections

    def test_invalid_collection_name_raises(self, db):
        with pytest.raises(ValueError):
            db.collection("invalid name!")

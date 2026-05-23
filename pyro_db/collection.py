"""
pyro_db.collection
==================
The ``Collection`` class — the primary interface for reading and writing
records.

Responsibilities
----------------
* Auto-generating sequential ``_id`` values backed by a per-collection
  atomic counter persisted in ``metadata.json``.
* Coordinating the storage engine, index manager, LRU cache, WAL, and lock.
* Enforcing schema validation (optional).
* Providing the full public CRUD API:

  - :meth:`create` — insert a new record.
  - :meth:`get` — fetch a single record by id or by field.
  - :meth:`update` — patch an existing record.
  - :meth:`delete` — soft-delete a record.
  - :meth:`filter` — query with operators.
  - :meth:`all` — return all live records.
  - :meth:`sort` — return all records sorted by a field.
  - :meth:`count` — return the number of live records.
  - :meth:`exists` — check if any record matches.
  - :meth:`compact` — rewrite the data file without stale versions.
  - :meth:`stats` — return collection statistics.

Transaction support
-------------------
:meth:`transaction` returns a context manager.  All writes inside the block
are buffered and committed atomically on ``__exit__``.  A rollback happens
automatically on exception.
"""

from __future__ import annotations

import copy
import json
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Union

from pyro_db.cache import LRUCache
from pyro_db.exceptions import (
    RecordNotFoundError,
    SchemaValidationError,
    TransactionError,
)
from pyro_db.indexes import IndexManager
from pyro_db.locks import CollectionLock
from pyro_db.query import QueryResult, _parse_kwargs
from pyro_db.schema import Schema
from pyro_db.storage import StorageEngine
from pyro_db.utils import now_ts
from pyro_db.wal import WAL


# ---------------------------------------------------------------------------
# ID counter helpers
# ---------------------------------------------------------------------------

class _IDCounter:
    """A thread-safe integer counter backed by a JSON file.

    The counter value is loaded from disk on construction and written back
    atomically on every increment.

    Parameters
    ----------
    path : Path
        Path to the ``<collection>_id.json`` file.
    """

    def __init__(self, path: Path):
        self._path = path
        self._lock = threading.Lock()
        self._value = self._load()

    def _load(self) -> int:
        """Read the current counter value from disk, defaulting to 0."""
        try:
            return json.loads(self._path.read_text(encoding="utf-8"))
        except (FileNotFoundError, ValueError, json.JSONDecodeError):
            return 0

    def _save(self) -> None:
        """Persist the current counter value atomically."""
        from pyro_db.utils import atomic_write
        atomic_write(self._path, str(self._value).encode("utf-8"))

    def next(self) -> int:
        """Increment and return the next ID.

        Returns
        -------
        int
            The next unique record ID (1-indexed).
        """
        with self._lock:
            self._value += 1
            self._save()
            return self._value

    @property
    def current(self) -> int:
        """The most recently issued ID (read-only)."""
        return self._value


# ---------------------------------------------------------------------------
# Transaction buffer
# ---------------------------------------------------------------------------

class _TransactionBuffer:
    """Accumulates write operations to be committed or rolled back atomically.

    Parameters
    ----------
    collection : Collection
        The collection this transaction belongs to.
    """

    def __init__(self, collection: "Collection"):
        self._collection = collection
        self._ops: List[dict] = []
        self._committed = False
        self._rolled_back = False

    def _ensure_open(self) -> None:
        if self._committed:
            raise TransactionError("Transaction has already been committed.")
        if self._rolled_back:
            raise TransactionError("Transaction has already been rolled back.")

    def create(self, **kwargs) -> dict:
        """Buffer a create operation.

        Returns
        -------
        dict
            A preview of the record that will be created on commit.
        """
        self._ensure_open()
        record = self._collection._build_record(kwargs)
        self._ops.append({"type": "create", "record": record})
        return {k: v for k, v in record.items() if not k.startswith("_")}

    def update(self, record_id: int, **kwargs) -> None:
        """Buffer an update operation."""
        self._ensure_open()
        self._ops.append({"type": "update", "id": record_id, "data": kwargs})

    def delete(self, record_id: int) -> None:
        """Buffer a delete operation."""
        self._ensure_open()
        self._ops.append({"type": "delete", "id": record_id})

    def commit(self) -> None:
        """Apply all buffered operations to the collection."""
        self._ensure_open()
        col = self._collection
        for op in self._ops:
            if op["type"] == "create":
                col._apply_create(op["record"])
            elif op["type"] == "update":
                col._apply_update(op["id"], op["data"])
            elif op["type"] == "delete":
                col._apply_delete(op["id"])
        self._committed = True

    def rollback(self) -> None:
        """Discard all buffered operations."""
        self._ensure_open()
        self._ops.clear()
        self._rolled_back = True


# ---------------------------------------------------------------------------
# Collection
# ---------------------------------------------------------------------------

class Collection:
    """A named collection of records within a :class:`~pyro_db.core.Database`.

    Instantiate via :meth:`~pyro_db.core.Database.collection`, not directly.

    Parameters
    ----------
    name : str
        Collection name (used as file-name stem).
    db_path : Path
        Root directory of the parent database.
    wal : WAL
        Shared Write-Ahead Log instance.
    encryptor : Encryptor | None
        Optional encryption layer.
    schema : Schema | None
        Optional schema for validation.
    unique_fields : list[str] | None
        Fields that must be unique across all records.
    cache_size : int
        Maximum number of records to hold in the LRU cache.
    lock_timeout : float
        Seconds to wait for the file lock before raising
        :class:`~pyro_db.exceptions.LockTimeoutError`.
    """

    def __init__(
        self,
        name: str,
        db_path: Path,
        wal: WAL,
        encryptor=None,
        schema: Optional[Schema] = None,
        unique_fields: Optional[List[str]] = None,
        cache_size: int = 1024,
        lock_timeout: float = 10.0,
    ):
        self._name = name
        self._db_path = Path(db_path)
        self._wal = wal
        self._encryptor = encryptor
        self._schema = schema

        col_base = self._db_path / name
        self._storage = StorageEngine(
            data_path=col_base.with_suffix(".data"),
            encryptor=encryptor,
        )
        self._index = IndexManager(
            index_path=col_base.with_suffix(".index"),
            unique_fields=unique_fields or [],
            encryptor=encryptor,
        )
        self._cache = LRUCache(max_size=cache_size)
        self._lock = CollectionLock(
            lock_path=col_base.with_suffix(".lock"),
            timeout=lock_timeout,
        )
        self._id_counter = _IDCounter(
            path=self._db_path / f"{name}_id.json"
        )
        self._recover()

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        """The name of this collection."""
        return self._name

    @property
    def schema(self) -> Optional[Schema]:
        """The optional :class:`~pyro_db.schema.Schema` attached to this collection."""
        return self._schema

    # ------------------------------------------------------------------
    # Crash recovery
    # ------------------------------------------------------------------

    def _recover(self) -> None:
        """Replay any pending WAL entries on startup to catch interrupted writes."""
        pending = self._wal.pending_entries(self._name)
        if not pending:
            return
        for entry in pending:
            op = entry["op"]
            data = entry.get("data")
            rid = entry.get("id")
            if op in ("CREATE", "UPDATE") and data:
                self._storage.append_record(data)
            elif op == "DELETE" and rid is not None:
                existing = self._storage.read_record(rid)
                if existing:
                    deleted = dict(existing)
                    deleted["_deleted"] = True
                    deleted["_updated"] = now_ts()
                    self._storage.append_record(deleted)
        # Rebuild index from recovered data.
        live = self._storage.load_live()
        self._index.rebuild(live)
        self._wal.log_checkpoint(self._name)

    # ------------------------------------------------------------------
    # Internal record building
    # ------------------------------------------------------------------

    def _build_record(self, data: dict) -> dict:
        """Build a complete internal record from user-supplied data.

        Applies schema defaults, validates, assigns a new ``_id``, and adds
        metadata timestamps.

        Parameters
        ----------
        data : dict
            User-supplied field data (no ``_``-prefixed keys).

        Returns
        -------
        dict
            A fully-formed internal record ready to be appended.

        Raises
        ------
        SchemaValidationError
            If schema validation fails.
        """
        if self._schema is not None:
            data = self._schema.apply_defaults(data)
            self._schema.validate(data, partial=False)
        ts = now_ts()
        record: dict = {
            "_id": self._id_counter.next(),
            "_created": ts,
            "_updated": ts,
            "_deleted": False,
        }
        record.update(data)
        return record

    # ------------------------------------------------------------------
    # Internal apply helpers (used by both direct writes and transactions)
    # ------------------------------------------------------------------

    def _apply_create(self, record: dict) -> dict:
        """Write a fully-formed record to the WAL, storage, and index."""
        with self._lock.write():
            self._wal.log_create(self._name, record)
            self._index.on_create(record)
            self._storage.append_record(record)
            self._cache.put(record["_id"], record)
            self._wal.log_checkpoint(self._name)
        return record

    def _apply_update(self, record_id: int, data: dict) -> dict:
        """Apply an update patch to an existing record."""
        with self._lock.write():
            old = self._storage.read_record(record_id)
            if old is None:
                raise RecordNotFoundError(record_id, self._name)
            if self._schema is not None:
                self._schema.validate(data, partial=True)
            new = dict(old)
            new.update(data)
            new["_updated"] = now_ts()
            self._wal.log_update(self._name, new)
            self._index.on_update(old, new)
            self._storage.append_record(new)
            self._cache.invalidate(record_id)
            self._cache.put(record_id, new)
            self._wal.log_checkpoint(self._name)
        return new

    def _apply_delete(self, record_id: int) -> dict:
        """Soft-delete a record."""
        with self._lock.write():
            existing = self._storage.read_record(record_id)
            if existing is None:
                raise RecordNotFoundError(record_id, self._name)
            deleted = dict(existing)
            deleted["_deleted"] = True
            deleted["_updated"] = now_ts()
            self._wal.log_delete(self._name, record_id)
            self._index.on_delete(existing)
            self._storage.append_record(deleted)
            self._cache.invalidate(record_id)
            self._wal.log_checkpoint(self._name)
        return deleted

    # ------------------------------------------------------------------
    # Public CRUD API
    # ------------------------------------------------------------------

    def create(self, **kwargs) -> dict:
        """Create a new record in the collection.

        Parameters
        ----------
        **kwargs
            Field values for the new record.

        Returns
        -------
        dict
            The created record with auto-assigned ``id`` and without internal
            metadata keys.

        Raises
        ------
        SchemaValidationError
            If the provided data fails schema validation.
        DuplicateIndexError
            If a unique-field constraint is violated.

        Examples
        --------
        ::

            user = users.create(username="alex", age=18)
            print(user["id"])   # 1
        """
        record = self._build_record(kwargs)
        stored = self._apply_create(record)
        return self._public_view(stored)

    def get(self, id: Optional[int] = None, **kwargs) -> dict:
        """Fetch a single record by ``id`` or by a field value.

        Parameters
        ----------
        id : int | None
            Record id to fetch.  If ``None``, *kwargs* must supply exactly
            one ``field=value`` pair to look up via the index.
        **kwargs
            Alternative: ``field=value`` lookup via the index.

        Returns
        -------
        dict
            The matching record (metadata keys stripped).

        Raises
        ------
        RecordNotFoundError
            If no matching live record is found.
        ValueError
            If both *id* and *kwargs* are provided, or neither is provided.

        Examples
        --------
        ::

            users.get(1)
            users.get(id=1)
            users.get(username="alex")
        """
        if id is not None and kwargs:
            raise ValueError("Supply either 'id' or a field=value lookup, not both.")
        if id is None and not kwargs:
            raise ValueError("Supply 'id' or a field=value pair to get().")

        if id is not None:
            record = self._fetch_by_id(id)
            if record is None:
                raise RecordNotFoundError(id, self._name)
            return self._public_view(record)

        # Field lookup via index.
        if len(kwargs) != 1:
            raise ValueError(
                "get() with field lookup supports exactly one field=value pair. "
                "Use filter() for multiple conditions."
            )
        field, value = next(iter(kwargs.items()))
        ids = self._index.lookup(field, value)
        with self._lock.read():
            live = self._storage.load_live()
        for rid in ids:
            rec = live.get(rid)
            if rec is not None:
                return self._public_view(rec)
        raise RecordNotFoundError(value, self._name)

    def update(self, id: int, **kwargs) -> dict:
        """Patch an existing record with new field values.

        Only the supplied fields are updated; all other fields are preserved.

        Parameters
        ----------
        id : int
            ID of the record to update.
        **kwargs
            Field values to patch.

        Returns
        -------
        dict
            The updated record (metadata keys stripped).

        Raises
        ------
        RecordNotFoundError
            If no live record exists with the given *id*.
        SchemaValidationError
            If the patched data fails schema validation.
        DuplicateIndexError
            If a unique-field constraint is violated.

        Examples
        --------
        ::

            users.update(1, age=19, role="admin")
        """
        updated = self._apply_update(id, kwargs)
        return self._public_view(updated)

    def delete(self, id: int) -> None:
        """Soft-delete the record with the given *id*.

        The record is marked as deleted and excluded from all future queries.
        It remains in the ``.data`` file until the next compaction.

        Parameters
        ----------
        id : int
            ID of the record to delete.

        Raises
        ------
        RecordNotFoundError
            If no live record exists with the given *id*.

        Examples
        --------
        ::

            users.delete(1)
        """
        self._apply_delete(id)

    # ------------------------------------------------------------------
    # Query API
    # ------------------------------------------------------------------

    def filter(self, **kwargs) -> QueryResult:
        """Return a :class:`~pyro_db.query.QueryResult` matching all conditions.

        Conditions use the ``field[__operator]=value`` syntax::

            users.filter(age=18)
            users.filter(age__gte=18, city="london")
            users.filter(username__startswith="al")

        Parameters
        ----------
        **kwargs
            Filter expressions.

        Returns
        -------
        QueryResult
            A chainable result set.

        Examples
        --------
        ::

            adults = users.filter(age__gte=18).sort("age").limit(10).all()
        """
        with self._lock.read():
            live = list(self._storage.load_live().values())
        result = QueryResult(live)
        if kwargs:
            result.filter(**kwargs)
        return result

    def all(self) -> List[dict]:
        """Return all live records in the collection.

        Returns
        -------
        list[dict]
            All non-deleted records (metadata stripped).

        Examples
        --------
        ::

            all_users = users.all()
        """
        return self.filter().all()

    def sort(self, field: str, descending: bool = False) -> QueryResult:
        """Return all live records sorted by *field*.

        Parameters
        ----------
        field : str
            Field to sort by.
        descending : bool
            ``True`` for Z → A / 9 → 0 order.

        Returns
        -------
        QueryResult
            A chainable, sorted result set.

        Examples
        --------
        ::

            users.sort("age", descending=True).limit(5).all()
        """
        return self.filter().sort(field, descending=descending)

    def count(self) -> int:
        """Return the total number of live records.

        Returns
        -------
        int

        Examples
        --------
        ::

            n = users.count()
        """
        with self._lock.read():
            return len(self._storage.load_live())

    def exists(self, **kwargs) -> bool:
        """Return ``True`` if any live record matches the given conditions.

        Parameters
        ----------
        **kwargs
            Filter expressions (same as :meth:`filter`).

        Returns
        -------
        bool

        Examples
        --------
        ::

            if users.exists(username="alex"):
                ...
        """
        return self.filter(**kwargs).exists()

    # ------------------------------------------------------------------
    # Batch operations
    # ------------------------------------------------------------------

    def create_many(self, records: List[dict]) -> List[dict]:
        """Insert multiple records in one call.

        Each record is validated and written individually.  The method returns
        after all records are committed; partial failures do not roll back
        previously committed records.

        Parameters
        ----------
        records : list[dict]
            List of field-value dicts to insert.

        Returns
        -------
        list[dict]
            List of created records (same order as input).

        Examples
        --------
        ::

            users.create_many([
                {"username": "alex", "age": 18},
                {"username": "bob",  "age": 25},
            ])
        """
        return [self.create(**rec) for rec in records]

    def delete_many(self, ids: List[int]) -> int:
        """Delete multiple records by ID.

        Parameters
        ----------
        ids : list[int]
            IDs to delete.

        Returns
        -------
        int
            Number of records actually deleted (IDs not found are skipped).
        """
        deleted = 0
        for rid in ids:
            try:
                self.delete(rid)
                deleted += 1
            except RecordNotFoundError:
                pass
        return deleted

    def update_many(self, ids: List[int], **kwargs) -> int:
        """Apply the same patch to multiple records.

        Parameters
        ----------
        ids : list[int]
            IDs to update.
        **kwargs
            Fields to patch.

        Returns
        -------
        int
            Number of records actually updated.
        """
        updated = 0
        for rid in ids:
            try:
                self.update(rid, **kwargs)
                updated += 1
            except RecordNotFoundError:
                pass
        return updated

    # ------------------------------------------------------------------
    # Transaction
    # ------------------------------------------------------------------

    @contextmanager
    def transaction(self) -> Generator[_TransactionBuffer, None, None]:
        """Context manager that batches writes into an atomic transaction.

        All operations within the block are buffered.  On a clean exit they
        are committed in order.  On any exception they are all discarded.

        Yields
        ------
        _TransactionBuffer
            A transaction proxy that accepts ``create``, ``update``, and
            ``delete`` calls.

        Raises
        ------
        TransactionError
            If commit fails unexpectedly.

        Examples
        --------
        ::

            with users.transaction() as txn:
                txn.create(username="alex", age=18)
                txn.update(2, role="admin")
                txn.delete(3)
        """
        txn = _TransactionBuffer(self)
        try:
            yield txn
            txn.commit()
        except Exception:
            txn.rollback()
            raise

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    def compact(self) -> dict:
        """Rewrite the data file, removing stale versions and deleted records.

        This operation:
        1. Acquires an exclusive write lock.
        2. Rewrites the ``.data`` file with only the latest live record per id.
        3. Rebuilds the index from the compacted data.
        4. Clears the LRU cache.
        5. Writes a WAL checkpoint.

        Returns
        -------
        dict
            ``{"records_remaining": int, "bytes_saved": int}``

        Examples
        --------
        ::

            result = users.compact()
            print(result["records_remaining"])
        """
        with self._lock.write():
            size_before = self._storage.file_size()
            live_count = self._storage.compact()
            live = self._storage.load_live()
            self._index.rebuild(live)
            self._cache.clear()
            self._wal.log_checkpoint(self._name)
            size_after = self._storage.file_size()
        return {
            "records_remaining": live_count,
            "bytes_saved": max(0, size_before - size_after),
        }

    def rebuild_index(self) -> None:
        """Force a full index rebuild from the data file.

        Useful after manual edits to the data file or when the index file is
        suspected to be corrupt.

        Examples
        --------
        ::

            users.rebuild_index()
        """
        with self._lock.write():
            live = self._storage.load_live()
            self._index.rebuild(live)

    def drop(self) -> None:
        """Permanently delete this collection and all its files.

        .. warning::
            This is irreversible. All data will be lost.

        Notes
        -----
        On Windows, a file cannot be deleted while any process holds an open
        handle to it.  The ``.lock`` sentinel file is kept open inside
        ``CollectionLock``, so we must release the lock (exit the ``with``
        block) *before* attempting to unlink it.

        Examples
        --------
        ::

            users.drop()
        """
        import os

        # Collect all paths up front.
        files_to_delete = [
            self._db_path / f"{self._name}{suffix}"
            for suffix in (".data", ".index", ".lock")
        ]
        files_to_delete.append(self._db_path / f"{self._name}_id.json")

        # Acquire the write lock only to safely clear in-memory state,
        # then let it go before touching the lock file itself.
        with self._lock.write():
            self._cache.clear()

        # Delete files *outside* the lock context so the OS handle on the
        # .lock file is fully closed before we try to unlink it (required
        # on Windows; harmless on POSIX).
        for path in files_to_delete:
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def stats(self) -> dict:
        """Return a dictionary of collection statistics.

        Returns
        -------
        dict
            Keys:

            * ``name`` — collection name.
            * ``record_count`` — number of live records.
            * ``file_size_bytes`` — size of the ``.data`` file.
            * ``indexed_fields`` — list of indexed field names.
            * ``cache`` — cache statistics dict from :meth:`LRUCache.stats`.
            * ``last_id`` — most recently assigned record ID.

        Examples
        --------
        ::

            print(users.stats())
        """
        with self._lock.read():
            live_count = len(self._storage.load_live())
            file_size = self._storage.file_size()
        return {
            "name": self._name,
            "record_count": live_count,
            "file_size_bytes": file_size,
            "indexed_fields": self._index.indexed_fields(),
            "cache": self._cache.stats(),
            "last_id": self._id_counter.current,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_by_id(self, record_id: int) -> Optional[dict]:
        """Fetch a single live record, trying the cache first.

        Parameters
        ----------
        record_id : int
            The ``_id`` to look up.

        Returns
        -------
        dict | None
            The live record, or ``None`` if not found or deleted.
        """
        cached = self._cache.get(record_id)
        if cached is not None and not cached.get("_deleted"):
            return cached
        with self._lock.read():
            record = self._storage.read_record(record_id)
        if record is not None:
            self._cache.put(record_id, record)
        return record

    @staticmethod
    def _public_view(record: dict) -> dict:
        """Return a copy of *record* without internal ``_``-prefixed keys.

        The ``_id`` field is re-exposed as ``id`` for a cleaner public API.

        Parameters
        ----------
        record : dict
            Internal record dict.

        Returns
        -------
        dict
            Public-facing record dict with ``id`` instead of ``_id``.
        """
        result = {k: v for k, v in record.items() if not k.startswith("_")}
        result["id"] = record["_id"]
        return result

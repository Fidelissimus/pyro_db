"""
pyro_db.indexes
===============
Hash-based field indexes for fast non-id lookups.

Architecture
------------
* An **in-memory index** is built at collection load time by scanning the
  ``.data`` file once.
* The index maps ``field → value → [record_id, ...]``.
* After every mutating operation the index is updated in memory **and** flushed
  to the ``.index`` file atomically.
* On startup the ``.index`` file is loaded instead of re-scanning the data
  file, making cold-start fast.

Index file format
-----------------
A single JSON object written with :func:`~pyro_db.utils.atomic_write`.
When an encryptor is supplied the entire JSON blob is AES-256-GCM encrypted
and stored as raw bytes; otherwise it is stored as readable UTF-8 JSON::

    {
      "username": {"alex": [1], "bob": [2]},
      "age":      {"18": [1, 3], "25": [2]}
    }

Values are always stored as strings in the JSON (even numeric values) because
JSON object keys must be strings.  The mapping handles the conversion.

Unique indexes
--------------
Fields can be designated *unique* (e.g. usernames, email addresses).  A
unique-index violation raises :class:`~pyro_db.exceptions.DuplicateIndexError`
before any write is committed.
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from pyro_db.exceptions import DuplicateIndexError
from pyro_db.utils import atomic_write


class IndexManager:
    """Manages field indexes for a single collection.

    Parameters
    ----------
    index_path : Path
        Path to the ``<collection>.index`` file.
    unique_fields : list[str] | None
        Field names that must be unique across all records.  Any create or
        update that would duplicate a value in one of these fields raises
        :class:`~pyro_db.exceptions.DuplicateIndexError`.
    encryptor : Encryptor | None
        Optional encryptor.  When supplied the entire index blob is encrypted
        before being written to disk and decrypted on load, so the ``.index``
        file is unreadable without the database password.
    """

    def __init__(
        self,
        index_path: Path,
        unique_fields: Optional[List[str]] = None,
        encryptor=None,
    ):
        self._path = Path(index_path)
        self._unique_fields: Set[str] = set(unique_fields or [])
        self._encryptor = encryptor
        # index structure: field -> str(value) -> [id, ...]
        self._data: Dict[str, Dict[str, List[int]]] = defaultdict(lambda: defaultdict(list))
        self._load()

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def path(self) -> Path:
        """Absolute path to the ``.index`` file."""
        return self._path

    @property
    def unique_fields(self) -> Set[str]:
        """Set of field names configured as unique."""
        return self._unique_fields

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _load(self) -> None:
        """Load the index from disk if the file exists.

        Decrypts the file contents when an encryptor is configured.
        Falls back to an empty index (which triggers a rebuild) if the
        file is missing, corrupt, or cannot be decrypted.
        """
        if not self._path.exists():
            return
        try:
            raw_bytes = self._path.read_bytes()
            if self._encryptor is not None:
                raw_bytes = self._encryptor.decrypt(raw_bytes)
            data = json.loads(raw_bytes.decode("utf-8")) if raw_bytes.strip() else {}
        except Exception:
            # Index is always re-buildable from the .data file.
            data = {}
        self._data = defaultdict(lambda: defaultdict(list))
        for field, val_map in data.items():
            for val_str, ids in val_map.items():
                self._data[field][val_str] = ids

    def flush(self) -> None:
        """Persist the current in-memory index to the ``.index`` file atomically.

        When an encryptor is configured the JSON blob is AES-256-GCM encrypted
        before being written, so the file is opaque without the password.
        The on-disk format converts the ``defaultdict`` to a regular ``dict``
        for clean JSON serialisation.
        """
        serialisable = {
            field: dict(val_map)
            for field, val_map in self._data.items()
        }
        raw = json.dumps(serialisable, ensure_ascii=False).encode("utf-8")
        if self._encryptor is not None:
            raw = self._encryptor.encrypt(raw)
        atomic_write(self._path, raw)

    # ------------------------------------------------------------------
    # Index manipulation
    # ------------------------------------------------------------------

    def rebuild(self, records: Dict[int, dict]) -> None:
        """Rebuild the entire index from a mapping of live records.

        This is called after compaction or crash-recovery to ensure the index
        matches the data file exactly.

        Parameters
        ----------
        records : dict[int, dict]
            Mapping of ``record_id → record`` for every **live** record.
        """
        self._data = defaultdict(lambda: defaultdict(list))
        for record in records.values():
            rid = record["_id"]
            for field, value in record.items():
                if field.startswith("_"):
                    continue
                self._add_to_index(field, value, rid)
        self.flush()

    def _add_to_index(self, field: str, value: Any, record_id: int) -> None:
        """Add *record_id* under ``field=value`` in the in-memory index.

        Parameters
        ----------
        field : str
            Field name.
        value : Any
            Field value (will be coerced to ``str`` as the dict key).
        record_id : int
            Record ID to add.
        """
        val_str = str(value)
        ids = self._data[field][val_str]
        if record_id not in ids:
            ids.append(record_id)

    def _remove_from_index(self, field: str, value: Any, record_id: int) -> None:
        """Remove *record_id* from ``field=value`` in the in-memory index.

        Parameters
        ----------
        field : str
            Field name.
        value : Any
            Field value.
        record_id : int
            Record ID to remove.
        """
        val_str = str(value)
        if field in self._data and val_str in self._data[field]:
            try:
                self._data[field][val_str].remove(record_id)
            except ValueError:
                pass
            # Clean up empty lists and dicts.
            if not self._data[field][val_str]:
                del self._data[field][val_str]
            if not self._data[field]:
                del self._data[field]

    # ------------------------------------------------------------------
    # Unique constraint checks
    # ------------------------------------------------------------------

    def check_unique(self, field: str, value: Any, exclude_id: Optional[int] = None) -> None:
        """Assert that ``field=value`` is not already present in the index.

        Parameters
        ----------
        field : str
            Field name to check.
        value : Any
            Proposed new value.
        exclude_id : int | None
            Record ID to exclude from the check (used during updates so the
            record can keep its own value).

        Raises
        ------
        DuplicateIndexError
            If another record already holds this value for *field*.
        """
        val_str = str(value)
        ids = self._data.get(field, {}).get(val_str, [])
        conflicting = [i for i in ids if i != exclude_id]
        if conflicting:
            raise DuplicateIndexError(field, value)

    # ------------------------------------------------------------------
    # Public update operations
    # ------------------------------------------------------------------

    def on_create(self, record: dict) -> None:
        """Update the index after a new record has been created.

        Also enforces unique constraints before adding.

        Parameters
        ----------
        record : dict
            The new record (must include ``_id``).

        Raises
        ------
        DuplicateIndexError
            If a unique field value already exists.
        """
        rid = record["_id"]
        for field in self._unique_fields:
            if field in record:
                self.check_unique(field, record[field], exclude_id=None)
        for field, value in record.items():
            if field.startswith("_"):
                continue
            self._add_to_index(field, value, rid)
        self.flush()

    def on_update(self, old_record: dict, new_record: dict) -> None:
        """Update the index after a record has been updated.

        Removes old field values from the index and adds new ones.

        Parameters
        ----------
        old_record : dict
            The previous version of the record.
        new_record : dict
            The updated record (same ``_id``).

        Raises
        ------
        DuplicateIndexError
            If a changed unique field value is already taken.
        """
        rid = old_record["_id"]
        for field in self._unique_fields:
            if field in new_record and new_record[field] != old_record.get(field):
                self.check_unique(field, new_record[field], exclude_id=rid)
        # Remove old values.
        for field, value in old_record.items():
            if field.startswith("_"):
                continue
            self._remove_from_index(field, value, rid)
        # Add new values.
        for field, value in new_record.items():
            if field.startswith("_"):
                continue
            self._add_to_index(field, value, rid)
        self.flush()

    def on_delete(self, record: dict) -> None:
        """Remove a record from the index after it has been deleted.

        Parameters
        ----------
        record : dict
            The record that was deleted (must include ``_id``).
        """
        rid = record["_id"]
        for field, value in record.items():
            if field.startswith("_"):
                continue
            self._remove_from_index(field, value, rid)
        self.flush()

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def lookup(self, field: str, value: Any) -> List[int]:
        """Return all record IDs where ``field == value``.

        Parameters
        ----------
        field : str
            Field to query.
        value : Any
            Value to match.

        Returns
        -------
        list[int]
            Ordered list of matching record IDs (may be empty).
        """
        val_str = str(value)
        return list(self._data.get(field, {}).get(val_str, []))

    def lookup_in(self, field: str, values: List[Any]) -> List[int]:
        """Return record IDs where ``field`` is one of *values*.

        Parameters
        ----------
        field : str
            Field to query.
        values : list[Any]
            List of acceptable values.

        Returns
        -------
        list[int]
            Deduplicated list of matching record IDs.
        """
        found: List[int] = []
        seen: Set[int] = set()
        for val in values:
            for rid in self.lookup(field, val):
                if rid not in seen:
                    found.append(rid)
                    seen.add(rid)
        return found

    def all_values(self, field: str) -> Dict[str, List[int]]:
        """Return the full value → ID mapping for *field*.

        Parameters
        ----------
        field : str
            Field name.

        Returns
        -------
        dict[str, list[int]]
            Mapping of stringified value to list of record IDs.
        """
        return dict(self._data.get(field, {}))

    def indexed_fields(self) -> List[str]:
        """Return a list of all currently indexed field names.

        Returns
        -------
        list[str]
            Sorted list of field names present in the index.
        """
        return sorted(self._data.keys())

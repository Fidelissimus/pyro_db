"""
pyro_db.core
============
The ``Database`` class — the top-level entry point for PyroDB.

Responsibilities
----------------
* Creating and managing the database directory.
* Persisting database-level metadata (version, encryption salt, collection
  registry) in ``metadata.json``.
* Instantiating and caching :class:`~pyro_db.collection.Collection` objects.
* Providing a database-level :meth:`~Database.close` and
  :meth:`~Database.compact_all` method.
* Optionally wrapping all I/O with :class:`~pyro_db.encryption.Encryptor`.

File structure
--------------
::

    <db_path>/
    ├── metadata.json          # database metadata (version, salt, collections)
    ├── wal.log                # shared Write-Ahead Log
    ├── <collection>.data      # JSONL record store
    ├── <collection>.index     # field → value → [id] index
    ├── <collection>.lock      # advisory file lock sentinel
    └── <collection>_id.json   # per-collection ID counter
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, List, Optional

from pyro_db.collection import Collection
from pyro_db.encryption import Encryptor
from pyro_db.exceptions import EncryptionError
from pyro_db.locks import DatabaseLock
from pyro_db.schema import Schema
from pyro_db.utils import atomic_write, validate_name
from pyro_db.wal import WAL

_METADATA_FILENAME = "metadata.json"
_DB_VERSION = "1"


class Database:
    """A PyroDB embedded database.

    Parameters
    ----------
    path : str | Path
        Directory where the database files are stored.  Created if it does
        not exist.
    password : str | None
        Optional password.  When provided, all data files and the WAL are
        encrypted with AES-256-GCM.  The password must be the same on every
        subsequent open.
    cache_size : int
        Maximum number of records to keep in the LRU cache **per collection**.
        Defaults to 1 024.
    lock_timeout : float
        Seconds to wait for a file lock before raising
        :class:`~pyro_db.exceptions.LockTimeoutError`.  Defaults to 10.

    Raises
    ------
    EncryptionError
        If *password* is supplied but the database was previously opened
        without encryption, or vice-versa.
    EncryptionError
        If *password* is wrong (detected via failed decryption of the metadata
        file).

    Examples
    --------
    ::

        db = Database("appdata")
        users = db.collection("users")

        db_enc = Database("appdata_enc", password="s3cr3t")
        users_enc = db_enc.collection("users")
    """

    def __init__(
        self,
        path: str | Path,
        password: Optional[str] = None,
        cache_size: int = 1024,
        lock_timeout: float = 10.0,
    ):
        self._path = Path(path).resolve()
        self._path.mkdir(parents=True, exist_ok=True)
        self._cache_size = cache_size
        self._lock_timeout = lock_timeout
        self._collections: Dict[str, Collection] = {}
        self._db_lock = DatabaseLock(self._path, timeout=lock_timeout * 3)

        # Initialise encryption.
        self._encryptor: Optional[Encryptor] = None
        if password is not None:
            self._encryptor = self._init_encryption(password)

        # Load / create metadata.
        self._metadata = self._load_or_create_metadata()

        # Initialise WAL.
        self._wal = WAL(self._path, encryptor=self._encryptor)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def path(self) -> Path:
        """Absolute path to the database directory."""
        return self._path

    @property
    def is_encrypted(self) -> bool:
        """``True`` if this database is encryption-enabled."""
        return self._encryptor is not None

    # ------------------------------------------------------------------
    # Encryption initialisation
    # ------------------------------------------------------------------

    def _salt_path(self) -> Path:
        return self._path / ".salt"

    def _init_encryption(self, password: str) -> Encryptor:
        """Create or reload the encryption key.

        On the first open, a random salt is generated and persisted.
        On subsequent opens the existing salt is loaded to reproduce the key.

        Parameters
        ----------
        password : str
            The user-supplied password.

        Returns
        -------
        Encryptor
            Ready-to-use encryptor for this database.
        """
        salt_path = self._salt_path()
        if salt_path.exists():
            salt = salt_path.read_bytes()
        else:
            from pyro_db.encryption import generate_salt
            salt = generate_salt()
            atomic_write(salt_path, salt)
        return Encryptor(password, salt)

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    def _meta_path(self) -> Path:
        return self._path / _METADATA_FILENAME

    def _load_or_create_metadata(self) -> dict:
        """Read metadata.json or create it if this is a new database.

        Returns
        -------
        dict
            Parsed metadata mapping.
        """
        meta_path = self._meta_path()
        if meta_path.exists():
            try:
                raw = meta_path.read_text(encoding="utf-8")
                metadata = json.loads(raw)
                # Verify that encryption mode matches.
                stored_encrypted = metadata.get("encrypted", False)
                if stored_encrypted and self._encryptor is None:
                    raise EncryptionError(
                        "This database was created with encryption enabled. "
                        "Supply a 'password' to open it."
                    )
                if not stored_encrypted and self._encryptor is not None:
                    raise EncryptionError(
                        "This database was created without encryption. "
                        "Open it without a 'password', or create a new database."
                    )
                return metadata
            except json.JSONDecodeError as exc:
                raise EncryptionError(
                    "metadata.json is corrupt or the password is wrong."
                ) from exc
        # New database — write initial metadata.
        metadata = {
            "version": _DB_VERSION,
            "encrypted": self._encryptor is not None,
            "collections": [],
        }
        self._save_metadata(metadata)
        return metadata

    def _save_metadata(self, metadata: Optional[dict] = None) -> None:
        """Persist metadata to disk atomically.

        Parameters
        ----------
        metadata : dict | None
            The metadata dict to save.  Defaults to ``self._metadata``.
        """
        if metadata is None:
            metadata = self._metadata
        raw = json.dumps(metadata, indent=2, ensure_ascii=False)
        atomic_write(self._meta_path(), raw.encode("utf-8"))

    def _register_collection(self, name: str) -> None:
        """Add *name* to the collection registry if not already present."""
        if name not in self._metadata.get("collections", []):
            self._metadata.setdefault("collections", []).append(name)
            self._save_metadata()

    # ------------------------------------------------------------------
    # Collection API
    # ------------------------------------------------------------------

    def collection(
        self,
        name: str,
        schema: Optional[Schema] = None,
        unique_fields: Optional[List[str]] = None,
        cache_size: Optional[int] = None,
    ) -> Collection:
        """Return (or create) the named collection.

        Calling this method multiple times with the same *name* returns the
        same :class:`~pyro_db.collection.Collection` instance.

        Parameters
        ----------
        name : str
            Collection name.  Must be a valid Python identifier (ASCII
            letters, digits, underscores; up to 64 chars).
        schema : Schema | None
            Optional :class:`~pyro_db.schema.Schema` for validation.
        unique_fields : list[str] | None
            Fields that must be unique across all records.
        cache_size : int | None
            Override the database-level cache size for this collection.

        Returns
        -------
        Collection

        Raises
        ------
        ValueError
            If *name* is not a valid identifier.

        Examples
        --------
        ::

            users = db.collection("users")
            posts = db.collection(
                "posts",
                schema=post_schema,
                unique_fields=["slug"],
            )
        """
        validate_name(name, "collection name")
        if name in self._collections:
            return self._collections[name]
        col = Collection(
            name=name,
            db_path=self._path,
            wal=self._wal,
            encryptor=self._encryptor,
            schema=schema,
            unique_fields=unique_fields,
            cache_size=cache_size if cache_size is not None else self._cache_size,
            lock_timeout=self._lock_timeout,
        )
        self._collections[name] = col
        self._register_collection(name)
        return col

    def list_collections(self) -> List[str]:
        """Return the names of all collections registered in this database.

        Returns
        -------
        list[str]
            Sorted list of collection names.

        Examples
        --------
        ::

            print(db.list_collections())   # ['posts', 'users']
        """
        return sorted(self._metadata.get("collections", []))

    def drop_collection(self, name: str) -> None:
        """Permanently delete a collection and all its files.

        .. warning::
            This is irreversible.

        Parameters
        ----------
        name : str
            Name of the collection to drop.

        Notes
        -----
        On Windows, file handles must be released before the OS allows
        deletion.  We ensure the ``Collection`` object (which holds the lock)
        is fully discarded before ``Collection.drop()`` deletes the files,
        and we never hold a ``Collection`` reference into the delete step.

        Examples
        --------
        ::

            db.drop_collection("temp_data")
        """
        import os

        # Resolve the Collection instance (cached or temporary).
        if name in self._collections:
            col = self._collections.pop(name)
        else:
            col = Collection(name, self._path, self._wal, self._encryptor)

        # drop() releases its internal lock before deleting files.
        col.drop()

        if name in self._metadata.get("collections", []):
            self._metadata["collections"].remove(name)
            self._save_metadata()

    # ------------------------------------------------------------------
    # Database-level operations
    # ------------------------------------------------------------------

    def compact_all(self) -> Dict[str, dict]:
        """Compact every registered collection.

        Acquires the database-level exclusive lock during the operation.

        Returns
        -------
        dict[str, dict]
            Mapping of collection name → compaction result dict.

        Examples
        --------
        ::

            results = db.compact_all()
            for name, info in results.items():
                print(name, info)
        """
        results: Dict[str, dict] = {}
        with self._db_lock.acquire():
            for name in self.list_collections():
                col = self.collection(name)
                results[name] = col.compact()
            self._wal.compact()
        return results

    def stats(self) -> dict:
        """Return a high-level statistics dict for the entire database.

        Returns
        -------
        dict
            Keys:

            * ``path`` — database directory path.
            * ``version`` — PyroDB format version string.
            * ``encrypted`` — bool.
            * ``collections`` — list of per-collection stat dicts.
            * ``total_records`` — sum of all live records.
            * ``total_bytes`` — sum of all ``.data`` file sizes.

        Examples
        --------
        ::

            print(db.stats())
        """
        collection_stats = []
        total_records = 0
        total_bytes = 0
        for name in self.list_collections():
            col = self.collection(name)
            s = col.stats()
            collection_stats.append(s)
            total_records += s["record_count"]
            total_bytes += s["file_size_bytes"]
        return {
            "path": str(self._path),
            "version": self._metadata.get("version", _DB_VERSION),
            "encrypted": self.is_encrypted,
            "collections": collection_stats,
            "total_records": total_records,
            "total_bytes": total_bytes,
        }

    def close(self) -> None:
        """Flush all pending state and release resources.

        After calling ``close`` the database object should not be used.
        It is safe (and recommended) to call this explicitly; the garbage
        collector will not do it for you.

        Examples
        --------
        ::

            db.close()
        """
        # All writes are synchronous and WAL checkpoints are written after
        # each operation, so there is nothing extra to flush.  We simply
        # clear the collection cache to release file handles.
        self._collections.clear()

    def __enter__(self) -> "Database":
        """Support use as a context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Close the database when the ``with`` block exits."""
        self.close()
        return None

    def __repr__(self) -> str:
        enc = " [encrypted]" if self.is_encrypted else ""
        cols = len(self._metadata.get("collections", []))
        return f"<Database path={str(self._path)!r} collections={cols}{enc}>"

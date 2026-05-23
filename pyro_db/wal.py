"""
pyro_db.wal
===========
Write-Ahead Log (WAL) for crash-recovery.

Every mutating operation is written to the WAL *before* it is applied to the
``.data`` file.  On startup, the WAL is replayed so that any operation that
was logged but not yet flushed to the data file is re-applied.

WAL file format
---------------
Each entry is a single line of JSON::

    {"op": "CREATE", "col": "users", "id": 1, "ts": 1700000000, "data": {...}}
    {"op": "UPDATE", "col": "users", "id": 1, "ts": 1700000001, "data": {...}}
    {"op": "DELETE", "col": "users", "id": 1, "ts": 1700000002}
    {"op": "CHECKPOINT", "col": "users", "ts": 1700000010}

A ``CHECKPOINT`` entry marks that all preceding entries for that collection
have been safely persisted to the ``.data`` file; they may be discarded on
the next compaction of the WAL.

Crash recovery
--------------
On :meth:`WAL.recover` the log is read in order.  For each collection we
collect all operations after the most recent ``CHECKPOINT``.  These are
replayed against the storage engine.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Iterator, List, Optional

from pyro_db.utils import atomic_append, now_ts


# ---------------------------------------------------------------------------
# Entry helpers
# ---------------------------------------------------------------------------

def _make_entry(op: str, collection: str, record_id=None, data: Optional[dict] = None) -> bytes:
    """Build a single WAL entry as a JSON bytes line.

    Parameters
    ----------
    op : str
        Operation string: ``"CREATE"``, ``"UPDATE"``, ``"DELETE"``, or
        ``"CHECKPOINT"``.
    collection : str
        Name of the target collection.
    record_id : int | str | None
        ID of the affected record (``None`` for ``CHECKPOINT``).
    data : dict | None
        Full record snapshot (required for ``CREATE`` and ``UPDATE``).

    Returns
    -------
    bytes
        UTF-8 encoded JSON line terminated with ``\\n``.
    """
    entry: dict = {"op": op, "col": collection, "ts": now_ts()}
    if record_id is not None:
        entry["id"] = record_id
    if data is not None:
        entry["data"] = data
    return (json.dumps(entry, separators=(",", ":")) + "\n").encode("utf-8")


# ---------------------------------------------------------------------------
# WAL
# ---------------------------------------------------------------------------

class WAL:
    """Manages the Write-Ahead Log for the entire database.

    Parameters
    ----------
    db_path : Path
        Root directory of the database.  The WAL file is stored as
        ``<db_path>/wal.log``.
    encryptor : Encryptor | None
        Optional :class:`~pyro_db.encryption.Encryptor`.  When provided,
        each WAL entry is encrypted individually before being written, and
        decrypted on read.

    Notes
    -----
    The WAL is shared across all collections.  Each entry carries a ``"col"``
    field so entries can be attributed to the correct collection during replay.
    """

    def __init__(self, db_path: Path, encryptor=None):
        self._path = Path(db_path) / "wal.log"
        self._encryptor = encryptor
        self._path.parent.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def path(self) -> Path:
        """Absolute path of the WAL file."""
        return self._path

    # ------------------------------------------------------------------
    # Write helpers
    # ------------------------------------------------------------------

    def _write_entry(self, raw: bytes) -> None:
        """Append a raw bytes entry to the WAL file.

        If encryption is configured the bytes are encrypted before writing.

        Parameters
        ----------
        raw : bytes
            A JSON line (including trailing newline) to persist.
        """
        if self._encryptor is not None:
            # Encrypt and base64-encode so the result is still a line.
            import base64
            encrypted = self._encryptor.encrypt(raw)
            line = base64.b64encode(encrypted) + b"\n"
        else:
            line = raw
        atomic_append(self._path, line)

    def log_create(self, collection: str, record: dict) -> None:
        """Log a ``CREATE`` operation.

        Parameters
        ----------
        collection : str
            Collection name.
        record : dict
            Complete record dict (including ``_id`` and metadata keys).
        """
        self._write_entry(_make_entry("CREATE", collection, record["_id"], record))

    def log_update(self, collection: str, record: dict) -> None:
        """Log an ``UPDATE`` operation.

        Parameters
        ----------
        collection : str
            Collection name.
        record : dict
            Updated record dict (including ``_id``).
        """
        self._write_entry(_make_entry("UPDATE", collection, record["_id"], record))

    def log_delete(self, collection: str, record_id) -> None:
        """Log a ``DELETE`` operation.

        Parameters
        ----------
        collection : str
            Collection name.
        record_id : int | str
            ID of the record being deleted.
        """
        self._write_entry(_make_entry("DELETE", collection, record_id))

    def log_checkpoint(self, collection: str) -> None:
        """Write a ``CHECKPOINT`` marker for *collection*.

        This signals that all preceding WAL entries for *collection* have been
        durably persisted to the ``.data`` file.

        Parameters
        ----------
        collection : str
            Collection name.
        """
        self._write_entry(_make_entry("CHECKPOINT", collection))

    # ------------------------------------------------------------------
    # Read / replay
    # ------------------------------------------------------------------

    def _iter_entries(self) -> Iterator[dict]:
        """Yield parsed WAL entry dicts in chronological order.

        Corrupt or unreadable lines are skipped with a warning rather than
        causing a hard failure.

        Yields
        ------
        dict
            Parsed WAL entry.
        """
        if not self._path.exists():
            return
        with open(self._path, "rb") as fh:
            for lineno, raw_line in enumerate(fh, start=1):
                raw_line = raw_line.strip()
                if not raw_line:
                    continue
                try:
                    if self._encryptor is not None:
                        import base64
                        decrypted = self._encryptor.decrypt(base64.b64decode(raw_line))
                        raw_line = decrypted
                    entry = json.loads(raw_line)
                    yield entry
                except Exception as exc:
                    import warnings
                    warnings.warn(
                        f"WAL: skipping corrupt entry at line {lineno}: {exc}",
                        RuntimeWarning,
                        stacklevel=2,
                    )

    def pending_entries(self, collection: str) -> List[dict]:
        """Return all WAL entries for *collection* after its last CHECKPOINT.

        Parameters
        ----------
        collection : str
            Collection whose pending entries are requested.

        Returns
        -------
        list[dict]
            Ordered list of ``CREATE`` / ``UPDATE`` / ``DELETE`` entries that
            must be replayed to bring the data file up to date.
        """
        pending: List[dict] = []
        for entry in self._iter_entries():
            if entry.get("col") != collection:
                continue
            if entry["op"] == "CHECKPOINT":
                pending.clear()
            else:
                pending.append(entry)
        return pending

    def all_pending(self) -> dict[str, List[dict]]:
        """Return pending WAL entries grouped by collection name.

        Returns
        -------
        dict[str, list[dict]]
            Mapping of ``collection_name → [entry, ...]``.
        """
        by_col: dict[str, List[dict]] = {}
        for entry in self._iter_entries():
            col = entry.get("col", "__unknown__")
            if entry["op"] == "CHECKPOINT":
                by_col[col] = []
            else:
                by_col.setdefault(col, []).append(entry)
        return by_col

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    def truncate(self) -> None:
        """Delete the WAL file entirely.

        Called after a successful full compaction when all data is known to be
        safely persisted to the ``.data`` files.
        """
        try:
            os.unlink(self._path)
        except FileNotFoundError:
            pass

    def compact(self) -> None:
        """Rewrite the WAL keeping only entries after the last CHECKPOINT
        for each collection.

        This prevents the WAL from growing without bound while still
        preserving any operations that have not yet been checkpointed.
        """
        by_col = self.all_pending()
        new_lines: List[bytes] = []
        for col, entries in by_col.items():
            # Write a synthetic checkpoint first so that on the next compact
            # the prefix is clean.
            new_lines.append(_make_entry("CHECKPOINT", col))
            for entry in entries:
                op = entry["op"]
                rid = entry.get("id")
                data = entry.get("data")
                new_lines.append(_make_entry(op, col, rid, data))

        if new_lines:
            from pyro_db.utils import atomic_write
            combined = b"".join(new_lines)
            if self._encryptor is not None:
                import base64
                enc_lines = []
                for line in new_lines:
                    encrypted = self._encryptor.encrypt(line)
                    enc_lines.append(base64.b64encode(encrypted) + b"\n")
                combined = b"".join(enc_lines)
            atomic_write(self._path, combined)
        else:
            self.truncate()

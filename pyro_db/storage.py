"""
pyro_db.storage
===============
Append-only JSONL storage engine for a single collection.

Design
------
* Every record version is appended as a JSON line to the ``.data`` file.
* Reads scan the file once and keep only the **last** version of each ID,
  filtering out soft-deleted records (``_deleted: true``).
* Updates are appended, not in-place.  The old lines remain until compaction.
* Compaction rewrites the ``.data`` file keeping only the latest live version
  of each record.

Encryption
----------
When an :class:`~pyro_db.encryption.Encryptor` is provided each line is
encrypted individually.  Encrypted lines are stored as base-64 to stay
line-delimited.

Thread / process safety
-----------------------
All public methods that mutate the file must be called while the caller holds
the appropriate :class:`~pyro_db.locks.CollectionLock`.  This class does not
acquire locks itself.
"""

from __future__ import annotations

import base64
import json
import os
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

from pyro_db.exceptions import CorruptionError
from pyro_db.utils import atomic_append, atomic_write, encode_line, decode_line, now_ts


class StorageEngine:
    """Manages the ``.data`` file for a single collection.

    Parameters
    ----------
    data_path : Path
        Path to the ``<collection>.data`` file.
    encryptor : Encryptor | None
        Optional encryptor for at-rest encryption of each line.
    """

    def __init__(self, data_path: Path, encryptor=None):
        self._path = Path(data_path)
        self._encryptor = encryptor
        self._path.parent.mkdir(parents=True, exist_ok=True)
        if not self._path.exists():
            self._path.touch()

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def path(self) -> Path:
        """Absolute path to the underlying ``.data`` file."""
        return self._path

    # ------------------------------------------------------------------
    # Low-level I/O
    # ------------------------------------------------------------------

    def _encode(self, record: dict) -> bytes:
        """Serialise and optionally encrypt *record* to a single line.

        Parameters
        ----------
        record : dict
            Raw record dict to encode.

        Returns
        -------
        bytes
            UTF-8 bytes line terminated with ``\\n``.
        """
        raw = encode_line(record)
        if self._encryptor is not None:
            encrypted = self._encryptor.encrypt(raw)
            return base64.b64encode(encrypted) + b"\n"
        return raw

    def _decode(self, raw_line: bytes, lineno: int) -> Optional[dict]:
        """Deserialise one raw line from the data file.

        Parameters
        ----------
        raw_line : bytes
            A single line read from the ``.data`` file.
        lineno : int
            Line number used in error messages.

        Returns
        -------
        dict | None
            Parsed record or ``None`` for blank lines.

        Raises
        ------
        CorruptionError
            If the line cannot be parsed and *encryptor* is ``None`` (for
            encrypted files we skip corrupt lines with a warning).
        """
        raw_line = raw_line.strip()
        if not raw_line:
            return None
        try:
            if self._encryptor is not None:
                decrypted = self._encryptor.decrypt(base64.b64decode(raw_line))
                return decode_line(decrypted)
            return decode_line(raw_line)
        except Exception as exc:
            import warnings
            warnings.warn(
                f"Storage: skipping corrupt line {lineno} in '{self._path}': {exc}",
                RuntimeWarning,
                stacklevel=3,
            )
            return None

    # ------------------------------------------------------------------
    # Public read API
    # ------------------------------------------------------------------

    def iter_raw(self) -> Iterator[Tuple[int, dict]]:
        """Yield ``(lineno, record)`` for every non-blank, parseable line.

        Yields
        ------
        tuple[int, dict]
            1-based line number and the parsed record dict.
        """
        if not self._path.exists():
            return
        with open(self._path, "rb") as fh:
            for lineno, raw_line in enumerate(fh, start=1):
                record = self._decode(raw_line, lineno)
                if record is not None:
                    yield lineno, record

    def load_all(self) -> Dict[int, dict]:
        """Read the entire data file and return a mapping of ID → latest record.

        Soft-deleted records (``_deleted == True``) are **included** in the
        returned mapping so that the index can be updated accordingly.  Callers
        that want live records only should filter by ``not r.get("_deleted")``.

        Returns
        -------
        dict[int, dict]
            Mapping of record ID to the most-recently-written version.
        """
        latest: Dict[int, dict] = {}
        for _, record in self.iter_raw():
            rid = record.get("_id")
            if rid is None:
                continue
            latest[rid] = record
        return latest

    def load_live(self) -> Dict[int, dict]:
        """Return only the live (non-deleted) records.

        Returns
        -------
        dict[int, dict]
            ID → record for every record where ``_deleted`` is falsy.
        """
        return {
            rid: rec
            for rid, rec in self.load_all().items()
            if not rec.get("_deleted", False)
        }

    def read_record(self, record_id: int) -> Optional[dict]:
        """Read the most recent version of a single record by ID.

        This performs a full file scan.  For hot paths, prefer the in-memory
        cache or the index.

        Parameters
        ----------
        record_id : int
            The ``_id`` to look up.

        Returns
        -------
        dict | None
            Most recent record version, or ``None`` if not found or deleted.
        """
        found: Optional[dict] = None
        for _, record in self.iter_raw():
            if record.get("_id") == record_id:
                found = record
        if found is None or found.get("_deleted", False):
            return None
        return found

    # ------------------------------------------------------------------
    # Public write API
    # ------------------------------------------------------------------

    def append_record(self, record: dict) -> None:
        """Append *record* as a new line in the data file.

        Parameters
        ----------
        record : dict
            Complete record dict (must include ``_id``).
        """
        atomic_append(self._path, self._encode(record))

    # ------------------------------------------------------------------
    # Compaction
    # ------------------------------------------------------------------

    def compact(self) -> int:
        """Rewrite the data file keeping only the latest live version of each record.

        Soft-deleted records are removed entirely.  The result is an atomic
        replacement of the existing ``.data`` file.

        Returns
        -------
        int
            Number of live records remaining after compaction.
        """
        live = self.load_live()
        lines = b"".join(self._encode(rec) for rec in live.values())
        atomic_write(self._path, lines)
        return len(live)

    # ------------------------------------------------------------------
    # File stats
    # ------------------------------------------------------------------

    def file_size(self) -> int:
        """Return the current size of the ``.data`` file in bytes.

        Returns
        -------
        int
            File size in bytes, or 0 if the file does not exist.
        """
        try:
            return self._path.stat().st_size
        except FileNotFoundError:
            return 0

    def line_count(self) -> int:
        """Return the total number of non-blank lines in the data file.

        This includes old versions of records and soft-deleted records.

        Returns
        -------
        int
            Number of parseable lines.
        """
        count = 0
        for _ in self.iter_raw():
            count += 1
        return count

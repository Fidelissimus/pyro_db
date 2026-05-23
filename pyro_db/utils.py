"""
pyro_db.utils
=============
Shared utility helpers used across the PyroDB codebase.

Responsibilities
----------------
* Atomic file writes (write-then-rename pattern).
* Timestamp generation.
* Safe JSON serialisation / deserialisation of a single JSONL line.
* ID counter helpers.
* Path sanitisation.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
import time
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Timestamp
# ---------------------------------------------------------------------------

def now_ts() -> int:
    """Return the current UTC timestamp as an integer (seconds since epoch).

    Returns
    -------
    int
        Current POSIX timestamp truncated to the nearest second.
    """
    return int(time.time())


# ---------------------------------------------------------------------------
# JSON helpers
# ---------------------------------------------------------------------------

def encode_line(record: dict) -> bytes:
    """Serialise *record* to a compact JSON bytes line terminated with ``\\n``.

    Parameters
    ----------
    record : dict
        Any JSON-serialisable mapping.

    Returns
    -------
    bytes
        UTF-8 encoded JSON followed by a newline character.
    """
    return (json.dumps(record, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")


def decode_line(raw: bytes | str) -> dict | None:
    """Deserialise one JSONL line.

    Parameters
    ----------
    raw : bytes | str
        A single JSON line, optionally with a trailing newline.

    Returns
    -------
    dict | None
        Parsed mapping, or ``None`` if *raw* is empty or whitespace-only.

    Raises
    ------
    json.JSONDecodeError
        If the line contains invalid JSON.
    """
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    raw = raw.strip()
    if not raw:
        return None
    return json.loads(raw)


# ---------------------------------------------------------------------------
# Atomic file writes
# ---------------------------------------------------------------------------

def atomic_write(path: Path, data: bytes) -> None:
    """Write *data* to *path* atomically using a write-then-rename strategy.

    The data is first written to a sibling temporary file in the same
    directory, then ``os.replace`` is used to atomically move it into place.
    This guarantees that readers never see a partially-written file.

    Parameters
    ----------
    path : Path
        Destination file path.
    data : bytes
        Raw bytes to write.
    """
    path = Path(path)
    dir_ = path.parent
    dir_.mkdir(parents=True, exist_ok=True)

    fd, tmp_path = tempfile.mkstemp(dir=dir_, prefix=".tmp_")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, path)
    except Exception:
        # Clean up temp file on failure so we don't litter the directory.
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def atomic_append(path: Path, data: bytes) -> None:
    """Append *data* to *path*, creating the file if it does not exist.

    Unlike :func:`atomic_write` this is **not** atomic at the OS level, but
    it is safe for our append-only storage model because:

    * We only ever append complete, newline-terminated JSON lines.
    * Incomplete trailing lines are ignored during reads.
    * The WAL provides crash-recovery on top of this.

    Parameters
    ----------
    path : Path
        Destination file path.
    data : bytes
        Raw bytes to append (must end with ``\\n``).
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "ab") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())


# ---------------------------------------------------------------------------
# Name sanitisation
# ---------------------------------------------------------------------------

_SAFE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")


def validate_name(name: str, label: str = "name") -> str:
    """Ensure *name* is a safe identifier suitable for use as a filename stem.

    Allowed characters: ASCII letters, digits, and underscores.
    Must start with a letter or underscore. Maximum 64 characters.

    Parameters
    ----------
    name : str
        The candidate name string.
    label : str
        Human-readable label used in the error message (e.g. "collection name").

    Returns
    -------
    str
        *name* unchanged if it is valid.

    Raises
    ------
    ValueError
        If *name* does not match the allowed pattern.
    """
    if not _SAFE_NAME_RE.match(name):
        raise ValueError(
            f"Invalid {label} {name!r}. "
            "Must start with a letter or underscore, contain only ASCII "
            "letters, digits, or underscores, and be at most 64 characters long."
        )
    return name


# ---------------------------------------------------------------------------
# Deep merge
# ---------------------------------------------------------------------------

def deep_merge(base: dict, updates: dict) -> dict:
    """Return a new dict that merges *updates* into *base* (non-destructive).

    Nested dicts are merged recursively.  All other types are replaced.

    Parameters
    ----------
    base : dict
        Original mapping.
    updates : dict
        Mapping of updates to apply on top of *base*.

    Returns
    -------
    dict
        New merged mapping; *base* and *updates* are not mutated.
    """
    result = dict(base)
    for key, value in updates.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


# ---------------------------------------------------------------------------
# Misc helpers
# ---------------------------------------------------------------------------

def clamp(value: int, lo: int, hi: int) -> int:
    """Clamp *value* to the inclusive range [*lo*, *hi*].

    Parameters
    ----------
    value : int
        The integer to clamp.
    lo : int
        Lower bound (inclusive).
    hi : int
        Upper bound (inclusive).

    Returns
    -------
    int
        *value* clamped to [*lo*, *hi*].
    """
    return max(lo, min(hi, value))


def strip_internal_keys(record: dict) -> dict:
    """Return a copy of *record* with all PyroDB-internal keys removed.

    Internal keys are those that start with ``_`` (underscore).

    Parameters
    ----------
    record : dict
        A raw storage record that may include internal metadata.

    Returns
    -------
    dict
        A new dict without any underscore-prefixed keys.
    """
    return {k: v for k, v in record.items() if not k.startswith("_")}

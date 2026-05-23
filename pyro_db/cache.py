"""
pyro_db.cache
=============
Least-Recently-Used (LRU) in-memory cache for deserialized records.

Motivation
----------
Disk I/O dominates read latency for a file-based database.  Caching recently
accessed records avoids repeated file scans for hot records.

Design
------
* Backed by ``collections.OrderedDict`` which maintains insertion order and
  supports efficient move-to-end operations — giving us O(1) LRU eviction.
* The cache stores **copies** of records.  Mutating a cached record does not
  affect the on-disk state (and vice-versa).
* ``max_size`` controls the maximum number of records kept in memory.  Once
  the limit is reached the least-recently-used record is evicted.
* Explicit invalidation is called by the collection on every write so the
  cache never serves stale data.

Thread safety
-------------
All public methods are protected by a ``threading.Lock``.  The cache is safe
to share across threads within one process.
"""

from __future__ import annotations

import copy
import threading
from collections import OrderedDict
from typing import Dict, Optional

_DEFAULT_MAX_SIZE = 1024


class LRUCache:
    """LRU cache for deserialized record dicts.

    Parameters
    ----------
    max_size : int
        Maximum number of records to keep in memory.  Defaults to 1 024.

    Examples
    --------
    ::

        cache = LRUCache(max_size=512)
        cache.put(1, {"_id": 1, "name": "alex"})
        record = cache.get(1)          # fast path — no disk I/O
        cache.invalidate(1)            # called after every write
        cache.clear()                  # called after compaction
    """

    def __init__(self, max_size: int = _DEFAULT_MAX_SIZE):
        if max_size < 1:
            raise ValueError(f"max_size must be at least 1, got {max_size}.")
        self._max_size = max_size
        self._store: OrderedDict[int, dict] = OrderedDict()
        self._lock = threading.Lock()
        self._hits = 0
        self._misses = 0

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    def get(self, record_id: int) -> Optional[dict]:
        """Return a deep copy of the cached record, or ``None`` on a miss.

        A cache hit moves the record to the most-recently-used position.

        Parameters
        ----------
        record_id : int
            The record ID to look up.

        Returns
        -------
        dict | None
            A deep copy of the record, or ``None`` if not cached.
        """
        with self._lock:
            if record_id not in self._store:
                self._misses += 1
                return None
            self._store.move_to_end(record_id)
            self._hits += 1
            return copy.deepcopy(self._store[record_id])

    def put(self, record_id: int, record: dict) -> None:
        """Insert or update *record* in the cache.

        A deep copy of *record* is stored so the caller can safely mutate the
        original without affecting the cache.

        Evicts the least-recently-used entry if the cache is full.

        Parameters
        ----------
        record_id : int
            The record ID (used as the cache key).
        record : dict
            The record to cache.
        """
        with self._lock:
            if record_id in self._store:
                self._store.move_to_end(record_id)
            self._store[record_id] = copy.deepcopy(record)
            if len(self._store) > self._max_size:
                self._store.popitem(last=False)

    def invalidate(self, record_id: int) -> None:
        """Remove *record_id* from the cache if present.

        Must be called after every write (create / update / delete) to prevent
        stale reads.

        Parameters
        ----------
        record_id : int
            The record ID to evict.
        """
        with self._lock:
            self._store.pop(record_id, None)

    def invalidate_many(self, record_ids) -> None:
        """Remove multiple record IDs from the cache.

        Parameters
        ----------
        record_ids : Iterable[int]
            Record IDs to evict.
        """
        with self._lock:
            for rid in record_ids:
                self._store.pop(rid, None)

    def clear(self) -> None:
        """Evict every record from the cache.

        Should be called after compaction, since the on-disk layout changes
        and any cached offsets or references may be invalid.
        """
        with self._lock:
            self._store.clear()

    # ------------------------------------------------------------------
    # Bulk warm-up
    # ------------------------------------------------------------------

    def warm(self, records: Dict[int, dict]) -> None:
        """Pre-populate the cache with a mapping of records.

        Records are inserted in the order provided.  If *records* exceeds
        ``max_size``, only the last ``max_size`` records are retained.

        Parameters
        ----------
        records : dict[int, dict]
            Mapping of ``record_id → record``.
        """
        for rid, rec in records.items():
            self.put(rid, rec)

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    @property
    def size(self) -> int:
        """Number of records currently held in the cache."""
        with self._lock:
            return len(self._store)

    @property
    def max_size(self) -> int:
        """Maximum number of records the cache will hold."""
        return self._max_size

    @property
    def hit_rate(self) -> float:
        """Cache hit rate as a float in [0.0, 1.0].

        Returns
        -------
        float
            Ratio of cache hits to total lookups.  Returns 0.0 if no lookups
            have been made yet.
        """
        with self._lock:
            total = self._hits + self._misses
            return self._hits / total if total else 0.0

    def stats(self) -> dict:
        """Return a summary of cache statistics.

        Returns
        -------
        dict
            Dictionary with keys ``size``, ``max_size``, ``hits``,
            ``misses``, and ``hit_rate``.
        """
        with self._lock:
            total = self._hits + self._misses
            return {
                "size": len(self._store),
                "max_size": self._max_size,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": self._hits / total if total else 0.0,
            }

    def reset_stats(self) -> None:
        """Reset hit/miss counters to zero."""
        with self._lock:
            self._hits = 0
            self._misses = 0

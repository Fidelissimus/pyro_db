"""
pyro_db.query
=============
Query builder and in-memory filter / sort / limit engine.

``QueryResult`` wraps a list of raw record dicts and exposes a fluent API::

    result = collection.filter(age__gte=18, city="london")
    result.sort("age", descending=True).limit(10).all()

Supported filter operators
--------------------------
============================  =========================================
Keyword syntax                Meaning
============================  =========================================
``field=value``               Exact equality
``field__eq=value``           Explicit equality (same as above)
``field__ne=value``           Not equal
``field__gt=value``           Greater than
``field__gte=value``          Greater than or equal
``field__lt=value``           Less than
``field__lte=value``          Less than or equal
``field__in=[v1, v2, ...]``   Value in list
``field__nin=[v1, v2, ...]``  Value not in list
``field__contains=sub``       String contains substring (case-sensitive)
``field__icontains=sub``      String contains substring (case-insensitive)
``field__startswith=prefix``  String starts with prefix
``field__endswith=suffix``    String ends with suffix
``field__exists=True/False``  Field present / absent
============================  =========================================
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Operator resolution
# ---------------------------------------------------------------------------

def _make_predicate(field: str, op: str, target: Any) -> Callable[[dict], bool]:
    """Build a single-field predicate function.

    Parameters
    ----------
    field : str
        Record field name.
    op : str
        Operator string (``"eq"``, ``"gt"``, ``"contains"``, etc.).
    target : Any
        The comparison value.

    Returns
    -------
    Callable[[dict], bool]
        A function that accepts a record dict and returns ``True`` if the
        record satisfies this predicate.

    Raises
    ------
    ValueError
        If *op* is not a recognised operator.
    """

    def _get(record: dict) -> Any:
        return record.get(field)

    ops: Dict[str, Callable[[dict], bool]] = {
        "eq":         lambda r: _get(r) == target,
        "ne":         lambda r: _get(r) != target,
        "gt":         lambda r: _get(r) is not None and _get(r) > target,
        "gte":        lambda r: _get(r) is not None and _get(r) >= target,
        "lt":         lambda r: _get(r) is not None and _get(r) < target,
        "lte":        lambda r: _get(r) is not None and _get(r) <= target,
        "in":         lambda r: _get(r) in target,
        "nin":        lambda r: _get(r) not in target,
        "contains":   lambda r: isinstance(_get(r), str) and target in _get(r),
        "icontains":  lambda r: isinstance(_get(r), str) and target.lower() in _get(r).lower(),
        "startswith": lambda r: isinstance(_get(r), str) and _get(r).startswith(target),
        "endswith":   lambda r: isinstance(_get(r), str) and _get(r).endswith(target),
        "exists":     lambda r: (field in r) == bool(target),
    }
    if op not in ops:
        raise ValueError(
            f"Unknown filter operator '{op}'. "
            f"Supported: {', '.join(sorted(ops))}."
        )
    return ops[op]


def _parse_kwargs(kwargs: dict) -> List[Callable[[dict], bool]]:
    """Convert keyword-argument filter expressions to a list of predicates.

    Each keyword takes the form ``field`` or ``field__operator``.  A bare
    ``field=value`` is treated as ``field__eq=value``.

    Parameters
    ----------
    kwargs : dict
        Mapping of filter expressions to their comparison values.

    Returns
    -------
    list[Callable[[dict], bool]]
        List of predicate functions; a record matches when ALL pass.

    Raises
    ------
    ValueError
        If any keyword uses an unrecognised operator.
    """
    predicates: List[Callable[[dict], bool]] = []
    for key, value in kwargs.items():
        parts = key.rsplit("__", 1)
        if len(parts) == 2:
            field, op = parts
        else:
            field, op = parts[0], "eq"
        predicates.append(_make_predicate(field, op, value))
    return predicates


# ---------------------------------------------------------------------------
# QueryResult
# ---------------------------------------------------------------------------

class QueryResult:
    """A lazy, chainable query result over an in-memory list of records.

    Parameters
    ----------
    records : list[dict]
        Initial list of **live** record dicts (internal metadata included).
    strip_meta : bool
        If ``True`` (the default), ``_``-prefixed metadata keys are removed
        from the dicts returned by :meth:`all` and :meth:`first`.

    Notes
    -----
    The filtering, sorting, and limiting steps are applied lazily when
    :meth:`all`, :meth:`first`, or :meth:`count` is called.
    """

    def __init__(self, records: List[dict], strip_meta: bool = True):
        self._records = records
        self._strip_meta = strip_meta
        self._predicates: List[Callable[[dict], bool]] = []
        self._sort_key: Optional[str] = None
        self._descending: bool = False
        self._limit_n: Optional[int] = None
        self._offset_n: int = 0

    # ------------------------------------------------------------------
    # Fluent query builder
    # ------------------------------------------------------------------

    def filter(self, **kwargs) -> "QueryResult":
        """Add additional filter expressions to this result.

        Each call is **additive** (all predicates are ANDed together).

        Parameters
        ----------
        **kwargs
            Filter expressions in the ``field[__operator]=value`` form.

        Returns
        -------
        QueryResult
            *self* for method chaining.
        """
        self._predicates.extend(_parse_kwargs(kwargs))
        return self

    def sort(self, field: str, descending: bool = False) -> "QueryResult":
        """Sort the result by a single field.

        Records that do not have *field* are sorted to the end.

        Parameters
        ----------
        field : str
            Field name to sort by.
        descending : bool
            ``True`` for descending (Z → A, 9 → 0) order.

        Returns
        -------
        QueryResult
            *self* for method chaining.
        """
        self._sort_key = field
        self._descending = descending
        return self

    def limit(self, n: int) -> "QueryResult":
        """Restrict the result to at most *n* records.

        Parameters
        ----------
        n : int
            Maximum number of records to return.

        Returns
        -------
        QueryResult
            *self* for method chaining.
        """
        if n < 0:
            raise ValueError(f"limit must be non-negative, got {n}.")
        self._limit_n = n
        return self

    def offset(self, n: int) -> "QueryResult":
        """Skip the first *n* records.

        Parameters
        ----------
        n : int
            Number of records to skip.

        Returns
        -------
        QueryResult
            *self* for method chaining.
        """
        if n < 0:
            raise ValueError(f"offset must be non-negative, got {n}.")
        self._offset_n = n
        return self

    # ------------------------------------------------------------------
    # Terminal operations
    # ------------------------------------------------------------------

    def _execute(self) -> List[dict]:
        """Apply all deferred operations and return the final record list."""
        result = list(self._records)

        # Apply all filter predicates.
        if self._predicates:
            result = [r for r in result if all(p(r) for p in self._predicates)]

        # Sort.
        if self._sort_key is not None:
            _key = self._sort_key

            def _sort_val(rec: dict):
                val = rec.get(_key)
                # Push None / missing to the end regardless of direction.
                if val is None:
                    return (1, None)
                return (0, val)

            try:
                result.sort(key=_sort_val, reverse=self._descending)
            except TypeError:
                # Mixed types (e.g. int and str) — fall back to string sort.
                result.sort(
                    key=lambda r: (0, str(r.get(_key, ""))) if r.get(_key) is not None else (1, ""),
                    reverse=self._descending,
                )

        # Offset then limit.
        if self._offset_n:
            result = result[self._offset_n:]
        if self._limit_n is not None:
            result = result[: self._limit_n]

        # Strip internal metadata if requested, but re-expose _id as id.
        if self._strip_meta:
            cleaned = []
            for r in result:
                row = {k: v for k, v in r.items() if not k.startswith("_")}
                if "_id" in r:
                    row["id"] = r["_id"]
                cleaned.append(row)
            return cleaned
        return result

    def all(self) -> List[dict]:
        """Execute the query and return all matching records.

        Returns
        -------
        list[dict]
            List of matching record dicts (metadata stripped if
            ``strip_meta=True``).
        """
        return self._execute()

    def first(self) -> Optional[dict]:
        """Execute the query and return the first matching record, or ``None``.

        Returns
        -------
        dict | None
            First matching record or ``None`` if no records match.
        """
        results = self._execute()
        return results[0] if results else None

    def count(self) -> int:
        """Return the number of records that match the current filter.

        Sorting and limit/offset are **not** applied to the count — it
        reflects the total number of matching records.

        Returns
        -------
        int
            Number of matching records.
        """
        records = list(self._records)
        if self._predicates:
            records = [r for r in records if all(p(r) for p in self._predicates)]
        return len(records)

    def exists(self) -> bool:
        """Return ``True`` if at least one record matches the query.

        Returns
        -------
        bool
        """
        for record in self._records:
            if all(p(record) for p in self._predicates):
                return True
        return False

    def pluck(self, *fields: str) -> List[Any]:
        """Return a flat list of values for a single field, or list-of-tuples
        for multiple fields.

        Parameters
        ----------
        *fields : str
            One or more field names to extract.

        Returns
        -------
        list
            If one field: list of values.  If multiple fields: list of tuples.
        """
        executed = self._execute()
        if len(fields) == 1:
            f = fields[0]
            return [r.get(f) for r in executed]
        return [tuple(r.get(f) for f in fields) for r in executed]

    def __iter__(self):
        """Iterate over all matching records."""
        return iter(self._execute())

    def __len__(self) -> int:
        """Return the number of matching records (same as :meth:`count`)."""
        return self.count()

    def __repr__(self) -> str:
        return (
            f"<QueryResult predicates={len(self._predicates)} "
            f"sort={self._sort_key!r} limit={self._limit_n}>"
        )

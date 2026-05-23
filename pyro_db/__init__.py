"""
PyroDB — A fast, encrypted, file-based NoSQL database engine for Python.

Usage::

    from pyro_db import Database

    db = Database("appdata")
    users = db.collection("users")

    users.create(username="alex", age=18)
    users.get(1)
    users.filter(age=18)
    users.update(1, age=19)
    users.delete(1)
"""

from pyro_db.core import Database
from pyro_db.collection import Collection
from pyro_db.exceptions import (
    PyroDB_Error,
    RecordNotFoundError,
    DuplicateIndexError,
    SchemaValidationError,
    TransactionError,
    EncryptionError,
    CorruptionError,
)

__version__ = "1.0.4"
__author__ = "PyroDB Contributors"
__license__ = "MIT"

__all__ = [
    "Database",
    "Collection",
    "PyroDB_Error",
    "RecordNotFoundError",
    "DuplicateIndexError",
    "SchemaValidationError",
    "TransactionError",
    "EncryptionError",
    "CorruptionError",
]

"""
pyro_db.exceptions
==================
All custom exception types used throughout PyroDB.

Every public-facing error inherits from ``PyroDB_Error`` so callers can
catch everything with a single ``except PyroDB_Error`` clause while still
being able to discriminate on the specific sub-type.
"""


class PyroDB_Error(Exception):
    """Base class for every PyroDB exception.

    All library-specific exceptions inherit from this class so users can
    distinguish PyroDB errors from other exceptions with a single base catch.
    """


class RecordNotFoundError(PyroDB_Error):
    """Raised when a requested record does not exist in the collection.

    Attributes
    ----------
    record_id : int | str
        The ID that was looked up and not found.
    collection : str
        The name of the collection that was searched.
    """

    def __init__(self, record_id, collection: str):
        self.record_id = record_id
        self.collection = collection
        super().__init__(
            f"Record with id={record_id!r} not found in collection '{collection}'."
        )


class DuplicateIndexError(PyroDB_Error):
    """Raised when a unique-index constraint is violated on insert or update.

    Attributes
    ----------
    field : str
        The indexed field that already holds ``value``.
    value : object
        The duplicate value that triggered the error.
    """

    def __init__(self, field: str, value):
        self.field = field
        self.value = value
        super().__init__(
            f"Unique index violation: field '{field}' already contains value {value!r}."
        )


class SchemaValidationError(PyroDB_Error):
    """Raised when a document fails schema validation.

    Attributes
    ----------
    errors : list[str]
        Human-readable list of validation failures.
    """

    def __init__(self, errors: list):
        self.errors = errors
        formatted = "; ".join(errors)
        super().__init__(f"Schema validation failed: {formatted}")


class TransactionError(PyroDB_Error):
    """Raised for transaction-related failures such as rollback errors or
    committing an already-committed transaction."""


class EncryptionError(PyroDB_Error):
    """Raised when encryption or decryption fails.

    This covers wrong passwords, corrupted ciphertext, and invalid key material.
    """


class CorruptionError(PyroDB_Error):
    """Raised when a data file or index is detected to be corrupt and cannot
    be recovered automatically.

    Attributes
    ----------
    path : str
        Path of the file found to be corrupt.
    """

    def __init__(self, path: str, detail: str = ""):
        self.path = path
        detail_msg = f" ({detail})" if detail else ""
        super().__init__(f"Data corruption detected in '{path}'{detail_msg}.")


class LockTimeoutError(PyroDB_Error):
    """Raised when acquiring a file lock exceeds the configured timeout.

    Attributes
    ----------
    path : str
        Path of the lock file that could not be acquired.
    timeout : float
        Number of seconds waited before giving up.
    """

    def __init__(self, path: str, timeout: float):
        self.path = path
        self.timeout = timeout
        super().__init__(
            f"Could not acquire lock on '{path}' within {timeout}s."
        )

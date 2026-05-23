"""
pyro_db.schema
==============
Lightweight schema definition and validation for collection records.

Schemas are **optional**.  A collection without a schema accepts any dict.
When a schema is attached, every ``create`` and ``update`` call is validated
before the record is written.

Design goals
------------
* Simple enough to define inline without external dependencies.
* Covers the most common real-world constraints: required fields, type
  checking, min/max (for numbers and strings), choices, and custom
  validators.
* Returns all validation errors at once rather than stopping at the first.

Usage example
-------------
::

    from pyro_db.schema import Schema, Field

    user_schema = Schema(
        username=Field(type=str, required=True, min_length=3, max_length=32),
        age=Field(type=int, required=False, min_value=0, max_value=150),
        role=Field(type=str, choices=["admin", "user", "guest"], default="user"),
    )

    db = Database("mydb")
    users = db.collection("users", schema=user_schema)
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List, Optional, Type, Union

from pyro_db.exceptions import SchemaValidationError


# ---------------------------------------------------------------------------
# Field descriptor
# ---------------------------------------------------------------------------

class Field:
    """Descriptor for a single schema field.

    Parameters
    ----------
    type : type | tuple[type, ...] | None
        Accepted Python type(s).  ``None`` means any type is allowed.
    required : bool
        If ``True`` the field must be present in every new record.  Defaults
        to ``False``.
    default : Any
        Default value to use when the field is absent on create.  If supplied
        the field is implicitly not required.  Use ``None`` to mean *no
        default*.
    min_value : int | float | None
        For numeric fields: minimum allowed value (inclusive).
    max_value : int | float | None
        For numeric fields: maximum allowed value (inclusive).
    min_length : int | None
        For string / list fields: minimum allowed length (inclusive).
    max_length : int | None
        For string / list fields: maximum allowed length (inclusive).
    choices : Iterable | None
        If supplied the field value must be one of the given choices.
    validator : Callable[[Any], str | None] | None
        Optional callable that receives the field value and returns an error
        message string on failure, or ``None`` on success.
    nullable : bool
        If ``True``, ``None`` is an accepted value regardless of *type*.
        Defaults to ``False``.
    """

    def __init__(
        self,
        type: Optional[Union[Type, tuple]] = None,
        *,
        required: bool = False,
        default: Any = ...,  # Ellipsis means "no default"
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None,
        choices: Optional[Iterable] = None,
        validator: Optional[Callable[[Any], Optional[str]]] = None,
        nullable: bool = False,
    ):
        self.type = type
        self.required = required if default is ... else False
        self.default = default
        self.has_default = default is not ...
        self.min_value = min_value
        self.max_value = max_value
        self.min_length = min_length
        self.max_length = max_length
        self.choices: Optional[list] = list(choices) if choices is not None else None
        self.validator = validator
        self.nullable = nullable

    def validate(self, field_name: str, value: Any) -> List[str]:
        """Validate *value* against this field's constraints.

        Parameters
        ----------
        field_name : str
            Used in error messages.
        value : Any
            The value to validate.

        Returns
        -------
        list[str]
            A (possibly empty) list of human-readable error strings.
        """
        errors: List[str] = []

        # Null check.
        if value is None:
            if self.nullable:
                return []
            if self.required:
                errors.append(f"'{field_name}' is required and cannot be None.")
                return errors
            return []

        # Type check.
        if self.type is not None and not isinstance(value, self.type):
            expected = (
                self.type.__name__
                if isinstance(self.type, type)
                else " | ".join(t.__name__ for t in self.type)
            )
            errors.append(
                f"'{field_name}' must be of type {expected}, got {type(value).__name__}."
            )
            # Skip further checks — they would produce misleading messages.
            return errors

        # Numeric range.
        if self.min_value is not None and isinstance(value, (int, float)):
            if value < self.min_value:
                errors.append(
                    f"'{field_name}' must be >= {self.min_value}, got {value}."
                )
        if self.max_value is not None and isinstance(value, (int, float)):
            if value > self.max_value:
                errors.append(
                    f"'{field_name}' must be <= {self.max_value}, got {value}."
                )

        # String / list length.
        if self.min_length is not None and hasattr(value, "__len__"):
            if len(value) < self.min_length:
                errors.append(
                    f"'{field_name}' length must be >= {self.min_length}, got {len(value)}."
                )
        if self.max_length is not None and hasattr(value, "__len__"):
            if len(value) > self.max_length:
                errors.append(
                    f"'{field_name}' length must be <= {self.max_length}, got {len(value)}."
                )

        # Choices.
        if self.choices is not None and value not in self.choices:
            errors.append(
                f"'{field_name}' must be one of {self.choices!r}, got {value!r}."
            )

        # Custom validator.
        if self.validator is not None:
            result = self.validator(value)
            if result:
                errors.append(f"'{field_name}': {result}")

        return errors


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

class Schema:
    """A collection-level schema composed of :class:`Field` descriptors.

    Parameters
    ----------
    **fields : Field
        Keyword arguments mapping field names to :class:`Field` instances.

    Attributes
    ----------
    fields : dict[str, Field]
        The fields defined in this schema.

    Examples
    --------
    ::

        schema = Schema(
            username=Field(type=str, required=True, min_length=2),
            age=Field(type=int, min_value=0, max_value=150),
            role=Field(type=str, choices=["admin", "user"], default="user"),
        )
    """

    def __init__(self, **fields: Field):
        self.fields: Dict[str, Field] = fields

    def apply_defaults(self, data: dict) -> dict:
        """Return a copy of *data* with missing field defaults filled in.

        Parameters
        ----------
        data : dict
            The user-supplied data dict.

        Returns
        -------
        dict
            New dict with defaults applied for any absent fields that have one.
        """
        result = dict(data)
        for name, field in self.fields.items():
            if name not in result and field.has_default:
                import copy
                result[name] = copy.deepcopy(field.default)
        return result

    def validate(self, data: dict, partial: bool = False) -> None:
        """Validate *data* against this schema.

        Parameters
        ----------
        data : dict
            The record data to validate (should not include ``_``-prefixed
            internal keys).
        partial : bool
            If ``True``, required-field checks are skipped.  Use this for
            ``update`` operations where only a subset of fields is provided.

        Raises
        ------
        SchemaValidationError
            If one or more validation errors are found.  The exception contains
            all errors, not just the first.
        """
        errors: List[str] = []

        for name, field in self.fields.items():
            if name in data:
                errors.extend(field.validate(name, data[name]))
            elif field.required and not partial:
                errors.append(f"'{name}' is a required field.")

        # Reject unknown fields if the schema is strict.
        # (Currently permissive — unknown fields pass through.)

        if errors:
            raise SchemaValidationError(errors)

    def __repr__(self) -> str:
        field_names = ", ".join(self.fields)
        return f"<Schema fields=[{field_names}]>"

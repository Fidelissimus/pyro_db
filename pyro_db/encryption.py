"""
pyro_db.encryption
==================
AES-256-GCM symmetric encryption with PBKDF2-SHA256 key derivation.

Design decisions
----------------
* **AES-GCM** provides authenticated encryption, meaning any tampering with
  the ciphertext is detected before decryption completes.
* **PBKDF2-SHA256** with 260 000 iterations converts a human-chosen password
  into a 256-bit key without requiring an external dependency (it is part of
  the Python standard library via ``hashlib``).
* A fresh **random 96-bit nonce** (IV) is generated for every encryption
  call, prepended to the ciphertext, and stripped on decryption.  Nonce
  reuse with AES-GCM is catastrophic, so we never reuse one.
* A random **16-byte salt** is derived once per database and stored in
  ``metadata.json``.  The same salt must be supplied on every open so we can
  reproduce the same derived key.

Wire format (per encrypted blob)
---------------------------------
::

    [ salt (16 bytes) ][ nonce (12 bytes) ][ tag (16 bytes) ][ ciphertext ]

The salt is embedded in every blob so that the module is self-contained and
blobs can be decrypted independently of the metadata file (useful for
disaster-recovery scenarios).
"""

from __future__ import annotations

import hashlib
import hmac
import os
import struct
from typing import Optional

from pyro_db.exceptions import EncryptionError

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SALT_SIZE = 16        # bytes
_NONCE_SIZE = 12       # bytes — 96-bit nonce recommended for AES-GCM
_TAG_SIZE = 16         # bytes — AES-GCM authentication tag
_KEY_SIZE = 32         # bytes — AES-256
_PBKDF2_ITERATIONS = 260_000
_PBKDF2_HASH = "sha256"

# Total header overhead prepended to every ciphertext blob:
# salt + nonce + tag
_HEADER_SIZE = _SALT_SIZE + _NONCE_SIZE + _TAG_SIZE


# ---------------------------------------------------------------------------
# Key derivation
# ---------------------------------------------------------------------------

def derive_key(password: str | bytes, salt: bytes) -> bytes:
    """Derive a 256-bit AES key from *password* and *salt* using PBKDF2-SHA256.

    Parameters
    ----------
    password : str | bytes
        The user-supplied password.  Strings are encoded to UTF-8 before
        hashing.
    salt : bytes
        A random 16-byte salt.  Must be the same value across all calls that
        need to produce the same key.

    Returns
    -------
    bytes
        32-byte derived key suitable for use with AES-256.
    """
    if isinstance(password, str):
        password = password.encode("utf-8")
    return hashlib.pbkdf2_hmac(
        _PBKDF2_HASH,
        password,
        salt,
        _PBKDF2_ITERATIONS,
        dklen=_KEY_SIZE,
    )


def generate_salt() -> bytes:
    """Generate a cryptographically-random 16-byte salt.

    Returns
    -------
    bytes
        16 random bytes suitable for use as a key-derivation salt.
    """
    return os.urandom(_SALT_SIZE)


# ---------------------------------------------------------------------------
# AES-GCM primitives (pure stdlib via ``cryptography`` package)
# ---------------------------------------------------------------------------

def _get_aesgcm():
    """Import ``cryptography.hazmat`` lazily so the module is importable even
    when the package is not installed (encryption will raise a clear error).

    Returns
    -------
    type
        The ``AESGCM`` class from the ``cryptography`` package.

    Raises
    ------
    EncryptionError
        If the ``cryptography`` package is not installed.
    """
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        return AESGCM
    except ImportError as exc:
        raise EncryptionError(
            "The 'cryptography' package is required for encryption. "
            "Install it with:  pip install cryptography"
        ) from exc


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

class Encryptor:
    """Handles encryption and decryption of arbitrary byte strings.

    Parameters
    ----------
    password : str | bytes
        The database password supplied by the user.
    salt : bytes | None
        16-byte salt for key derivation.  Pass ``None`` on first creation and
        retrieve the generated salt via :attr:`salt` to persist it.

    Attributes
    ----------
    salt : bytes
        The salt used for key derivation.  Persist this in ``metadata.json``
        so the same key can be re-derived on subsequent opens.
    """

    def __init__(self, password: str | bytes, salt: Optional[bytes] = None):
        self._salt: bytes = salt if salt is not None else generate_salt()
        self._key: bytes = derive_key(password, self._salt)
        AESGCM = _get_aesgcm()
        self._aesgcm = AESGCM(self._key)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def salt(self) -> bytes:
        """The key-derivation salt (16 bytes).

        Persist this alongside the database metadata.
        """
        return self._salt

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    def encrypt(self, plaintext: bytes) -> bytes:
        """Encrypt *plaintext* and return an authenticated ciphertext blob.

        A new random nonce is generated for each call.

        Wire format::

            salt (16) | nonce (12) | tag (16) | ciphertext

        The ``cryptography`` library's ``AESGCM.encrypt`` returns
        ``ciphertext + tag`` concatenated.  We rearrange to put the tag before
        the ciphertext for easier parsing.

        Parameters
        ----------
        plaintext : bytes
            Raw bytes to encrypt.

        Returns
        -------
        bytes
            Authenticated ciphertext blob that includes salt, nonce, and tag.

        Raises
        ------
        EncryptionError
            If encryption fails for any reason.
        """
        try:
            nonce = os.urandom(_NONCE_SIZE)
            # cryptography returns ciphertext || tag (tag is last 16 bytes)
            ct_with_tag = self._aesgcm.encrypt(nonce, plaintext, None)
            ciphertext = ct_with_tag[:-_TAG_SIZE]
            tag = ct_with_tag[-_TAG_SIZE:]
            return self._salt + nonce + tag + ciphertext
        except Exception as exc:
            raise EncryptionError(f"Encryption failed: {exc}") from exc

    def decrypt(self, blob: bytes) -> bytes:
        """Decrypt and authenticate *blob*, returning the original plaintext.

        Parameters
        ----------
        blob : bytes
            An authenticated ciphertext blob produced by :meth:`encrypt`.

        Returns
        -------
        bytes
            Original plaintext.

        Raises
        ------
        EncryptionError
            If the blob is too short, the password is wrong, or the
            ciphertext has been tampered with.
        """
        if len(blob) < _HEADER_SIZE:
            raise EncryptionError(
                f"Ciphertext blob is too short ({len(blob)} bytes); "
                f"minimum expected is {_HEADER_SIZE} bytes."
            )
        try:
            salt = blob[:_SALT_SIZE]
            nonce = blob[_SALT_SIZE: _SALT_SIZE + _NONCE_SIZE]
            tag = blob[_SALT_SIZE + _NONCE_SIZE: _HEADER_SIZE]
            ciphertext = blob[_HEADER_SIZE:]
            # Re-derive key in case this blob uses a different salt
            # (e.g. copied from another database with same password).
            if salt != self._salt:
                key = derive_key(self._key, salt)  # type: ignore[arg-type]
                AESGCM = _get_aesgcm()
                aesgcm = AESGCM(key)
            else:
                aesgcm = self._aesgcm
            # cryptography expects ciphertext || tag
            return aesgcm.decrypt(nonce, ciphertext + tag, None)
        except EncryptionError:
            raise
        except Exception as exc:
            raise EncryptionError(
                "Decryption failed — wrong password or corrupt data."
            ) from exc

    # ------------------------------------------------------------------
    # Convenience wrappers for text
    # ------------------------------------------------------------------

    def encrypt_str(self, plaintext: str) -> bytes:
        """Encrypt a UTF-8 string.

        Parameters
        ----------
        plaintext : str
            String to encrypt.

        Returns
        -------
        bytes
            Authenticated ciphertext blob.
        """
        return self.encrypt(plaintext.encode("utf-8"))

    def decrypt_str(self, blob: bytes) -> str:
        """Decrypt a blob to a UTF-8 string.

        Parameters
        ----------
        blob : bytes
            Authenticated ciphertext blob.

        Returns
        -------
        str
            Decrypted string.
        """
        return self.decrypt(blob).decode("utf-8")

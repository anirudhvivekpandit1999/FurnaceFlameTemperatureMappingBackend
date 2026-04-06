"""
crypto.py — Three-Layer Encryption for Furnace API
====================================================

LAYER 1  (Transit-IN  Decryption)  — AES-256-GCM
  • Client encrypts the request body before sending.
  • Middleware decrypts it so every endpoint receives plain data.
  • Algorithm: AES-256-GCM  (authenticated, tamper-proof)
  • Key env-var : TRANSIT_KEY  (32 raw bytes → base64url, or hex)

LAYER 2  (At-Rest Encryption)  — AES-256-CBC + HMAC-SHA256
  • Every sensitive decimal / string column is encrypted individually.
  • A fresh random 16-byte IV is prepended to each cipher-text blob.
  • Format stored in DB:  base64( IV[16] || CT || HMAC-SHA256[32] )
  • Two env-vars:
      REST_ENC_KEY  (32 bytes → base64url)   – AES key
      REST_MAC_KEY  (32 bytes → base64url)   – HMAC key  (encrypt-then-MAC)

LAYER 3  (Transit-OUT Encryption)  — AES-256-GCM
  • Every JSON response is encrypted before it leaves the server.
  • Client decrypts it on arrival.
  • Uses the same key as Layer 1 (or a separate TRANSIT_OUT_KEY if you
    prefer; currently falls back to TRANSIT_KEY for simplicity).
  • Wire format (JSON):
      { "iv": "<base64>", "tag": "<base64>", "ct": "<base64>" }

IMPORTANT — Key bootstrap
--------------------------
On first run (dev), if the env-vars are absent the module auto-generates
keys and prints them to stdout.  In production always set the env-vars.
"""

import os
import base64
import hashlib
import hmac
import json
import secrets
from typing import Any

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend


# ─────────────────────────────────────────────────────────────
# Key Loading / Bootstrap
# ─────────────────────────────────────────────────────────────

def _load_or_generate(env_var: str, label: str) -> bytes:
    """Return 32-byte key from env-var (base64 or hex).  Auto-generate in dev."""
    raw = os.getenv(env_var, "")
    if raw:
        try:
            key = base64.urlsafe_b64decode(raw + "==")  # tolerant padding
            if len(key) == 32:
                return key
        except Exception:
            pass
        try:
            key = bytes.fromhex(raw)
            if len(key) == 32:
                return key
        except Exception:
            pass
        raise ValueError(f"Env-var {env_var} must be 32 bytes encoded as base64url or hex.")

    # Auto-generate (dev only)
    key = secrets.token_bytes(32)
    encoded = base64.urlsafe_b64encode(key).decode()
    print(f"[CRYPTO] ⚠  {env_var} not set — generated key (add to .env):\n"
          f"  export {env_var}={encoded}\n")
    return key


TRANSIT_KEY:   bytes = _load_or_generate("TRANSIT_KEY",    "Transit (in/out) AES-GCM key")
REST_ENC_KEY:  bytes = _load_or_generate("REST_ENC_KEY",   "At-rest AES-CBC key")
REST_MAC_KEY:  bytes = _load_or_generate("REST_MAC_KEY",   "At-rest HMAC-SHA256 key")

# Optional separate out-key; falls back to TRANSIT_KEY
_transit_out_raw = os.getenv("TRANSIT_OUT_KEY", "")
TRANSIT_OUT_KEY: bytes = (
    _load_or_generate("TRANSIT_OUT_KEY", "Transit-OUT AES-GCM key")
    if _transit_out_raw else TRANSIT_KEY
)


# ─────────────────────────────────────────────────────────────
# LAYER 1 — Transit-IN  (AES-256-GCM decryption)
# ─────────────────────────────────────────────────────────────
#
# Expected wire format from client (JSON):
#   { "iv": "<base64>", "tag": "<base64>", "ct": "<base64>" }
#
# The client must use the shared TRANSIT_KEY to encrypt before sending.

class TransitDecryptionError(Exception):
    """Raised when the transit-IN cipher-text is malformed or authentication fails."""


def decrypt_transit_in(payload: dict) -> bytes:
    """
    Decrypt an AES-256-GCM payload sent by the client.

    Parameters
    ----------
    payload : dict
        Must contain keys "iv", "tag", "ct" (all base64-encoded).

    Returns
    -------
    bytes
        The original plaintext bytes (typically UTF-8 JSON).
    """
    try:
        iv  = base64.b64decode(payload["iv"])
        tag = base64.b64decode(payload["tag"])
        ct  = base64.b64decode(payload["ct"])
    except (KeyError, Exception) as exc:
        raise TransitDecryptionError(f"Malformed transit payload: {exc}") from exc

    aesgcm = AESGCM(TRANSIT_KEY)
    # cryptography library expects ct || tag concatenated
    try:
        plaintext = aesgcm.decrypt(iv, ct + tag, None)
    except Exception as exc:
        raise TransitDecryptionError(f"AES-GCM authentication failed: {exc}") from exc

    return plaintext


def decrypt_transit_in_json(payload: dict) -> Any:
    """Convenience wrapper — returns parsed Python object."""
    raw = decrypt_transit_in(payload)
    return json.loads(raw.decode("utf-8"))


# ─────────────────────────────────────────────────────────────
# LAYER 2 — At-Rest  (AES-256-CBC + HMAC-SHA256)
# ─────────────────────────────────────────────────────────────
#
# Storage format (base64 of):
#   IV[16]  ||  CT[...]  ||  HMAC[32]
#
# • IV   is random per-field per-write.
# • CT   is PKCS7-padded AES-256-CBC cipher-text.
# • HMAC is SHA-256 over (IV || CT) using REST_MAC_KEY  (encrypt-then-MAC).

_BLOCK = 16   # AES block size


def _pkcs7_pad(data: bytes) -> bytes:
    pad_len = _BLOCK - (len(data) % _BLOCK)
    return data + bytes([pad_len] * pad_len)


def _pkcs7_unpad(data: bytes) -> bytes:
    pad_len = data[-1]
    if pad_len < 1 or pad_len > _BLOCK:
        raise ValueError("Invalid PKCS7 padding")
    if data[-pad_len:] != bytes([pad_len] * pad_len):
        raise ValueError("Corrupt PKCS7 padding")
    return data[:-pad_len]


def _hmac_sha256(key: bytes, data: bytes) -> bytes:
    return hmac.new(key, data, hashlib.sha256).digest()


def encrypt_at_rest(value: Any) -> str | None:
    """
    Encrypt a single field value for storage.

    Parameters
    ----------
    value : Any
        Numeric or string value to encrypt.  None is passed through as None.

    Returns
    -------
    str | None
        Base64-encoded blob  "IV || CT || HMAC",  or None if value is None.
    """
    if value is None:
        return None

    plaintext = str(value).encode("utf-8")
    iv = secrets.token_bytes(_BLOCK)

    cipher = Cipher(
        algorithms.AES(REST_ENC_KEY),
        modes.CBC(iv),
        backend=default_backend()
    )
    encryptor = cipher.encryptor()
    ct = encryptor.update(_pkcs7_pad(plaintext)) + encryptor.finalize()

    mac = _hmac_sha256(REST_MAC_KEY, iv + ct)
    blob = iv + ct + mac
    return base64.b64encode(blob).decode("ascii")


def decrypt_at_rest(blob: str | None) -> str | None:
    """
    Decrypt a single at-rest field value.

    Parameters
    ----------
    blob : str | None
        Base64-encoded "IV || CT || HMAC" as stored in the DB.

    Returns
    -------
    str | None
        Original plaintext string, or None if blob is None.

    Raises
    ------
    ValueError
        If the HMAC does not match (tampering detected) or padding is corrupt.
    """
    if blob is None:
        return None

    raw = base64.b64decode(blob)
    if len(raw) < _BLOCK + 32 + _BLOCK:   # minimum: IV + 1 cipher-block + HMAC
        raise ValueError("At-rest blob too short")

    iv  = raw[:_BLOCK]
    mac = raw[-32:]
    ct  = raw[_BLOCK:-32]

    # Verify MAC first (timing-safe)
    expected_mac = _hmac_sha256(REST_MAC_KEY, iv + ct)
    if not hmac.compare_digest(expected_mac, mac):
        raise ValueError("At-rest HMAC verification failed — data may be tampered")

    cipher = Cipher(
        algorithms.AES(REST_ENC_KEY),
        modes.CBC(iv),
        backend=default_backend()
    )
    decryptor = cipher.decryptor()
    padded = decryptor.update(ct) + decryptor.finalize()
    return _pkcs7_unpad(padded).decode("utf-8")


def decrypt_at_rest_float(blob: str | None) -> float | None:
    """Decrypt and cast to float (for decimal DB columns)."""
    s = decrypt_at_rest(blob)
    return float(s) if s is not None else None


def decrypt_at_rest_int(blob: str | None) -> int | None:
    """Decrypt and cast to int."""
    s = decrypt_at_rest(blob)
    return int(s) if s is not None else None


# ─────────────────────────────────────────────────────────────
# LAYER 3 — Transit-OUT  (AES-256-GCM encryption)
# ─────────────────────────────────────────────────────────────
#
# Wire format (JSON string):
#   { "iv": "<base64>", "tag": "<base64>", "ct": "<base64>" }

def encrypt_transit_out(data: Any) -> dict:
    """
    Encrypt a Python object for transmission to the client.

    Parameters
    ----------
    data : Any
        JSON-serialisable object (dict, list, str, …).

    Returns
    -------
    dict
        { "iv": str, "tag": str, "ct": str }  — all base64-encoded.
    """
    plaintext = json.dumps(data, default=str).encode("utf-8")
    iv = secrets.token_bytes(12)   # GCM standard nonce: 12 bytes

    aesgcm = AESGCM(TRANSIT_OUT_KEY)
    ct_with_tag = aesgcm.encrypt(iv, plaintext, None)

    # cryptography appends the 16-byte tag at the end
    ct  = ct_with_tag[:-16]
    tag = ct_with_tag[-16:]

    return {
        "iv":  base64.b64encode(iv).decode(),
        "tag": base64.b64encode(tag).decode(),
        "ct":  base64.b64encode(ct).decode(),
    }


# ─────────────────────────────────────────────────────────────
# Convenience: encrypt a dict of DB field values
# ─────────────────────────────────────────────────────────────

def encrypt_row(row: dict) -> dict:
    """Return a new dict with every value passed through encrypt_at_rest."""
    return {k: encrypt_at_rest(v) for k, v in row.items()}


def decrypt_row(row: dict) -> dict:
    """Return a new dict with every value passed through decrypt_at_rest."""
    return {k: decrypt_at_rest(v) for k, v in row.items()}
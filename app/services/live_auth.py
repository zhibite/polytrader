"""L1 wallet authentication for Polymarket CLOB v2.

Adapted from 02.poly1h/src/clob_l1_auth.py: derive/create L2 API credentials
from a private key with EIP-712 ClobAuth. The SDK is imported lazily so the
paper-only deployment does not need py-clob-client-v2.
"""

from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass
class ClobApiCreds:
    api_key: str
    api_secret: str
    api_passphrase: str


class ClobL1Error(Exception):
    pass


def _build_l1_headers(signer, timestamp: int, nonce: int) -> dict:
    from py_clob_client_v2.headers.headers import (
        POLY_ADDRESS,
        POLY_NONCE,
        POLY_SIGNATURE,
        POLY_TIMESTAMP,
    )
    from py_clob_client_v2.signing.eip712 import sign_clob_auth_message

    signature = sign_clob_auth_message(signer, timestamp, nonce)
    return {
        POLY_ADDRESS: signer.address(),
        POLY_SIGNATURE: signature,
        POLY_TIMESTAMP: str(timestamp),
        POLY_NONCE: str(nonce),
    }


def _create_api_key_with_headers(host: str, headers: dict) -> ClobApiCreds:
    from py_clob_client_v2.endpoints import CREATE_API_KEY
    from py_clob_client_v2.http_helpers.helpers import post

    response = post(f"{host}{CREATE_API_KEY}", headers=headers)
    return ClobApiCreds(
        api_key=response["apiKey"],
        api_secret=response["secret"],
        api_passphrase=response["passphrase"],
    )


def _derive_api_key_with_headers(host: str, headers: dict) -> ClobApiCreds:
    from py_clob_client_v2.endpoints import DERIVE_API_KEY
    from py_clob_client_v2.http_helpers.helpers import get

    response = get(f"{host}{DERIVE_API_KEY}", headers=headers)
    return ClobApiCreds(
        api_key=response["apiKey"],
        api_secret=response["secret"],
        api_passphrase=response["passphrase"],
    )


def create_or_derive_clob_api_creds(
    host: str,
    private_key: str,
    chain_id: int = 137,
) -> ClobApiCreds:
    """Create API credentials, falling back to deriving existing ones."""
    try:
        from py_clob_client_v2.signer import Signer

        signer = Signer(private_key, chain_id)
        headers = _build_l1_headers(signer, int(time.time()), 0)
        try:
            return _create_api_key_with_headers(host, headers)
        except Exception:
            return _derive_api_key_with_headers(host, headers)
    except Exception as exc:
        raise ClobL1Error(f"L1 auth failed: {exc}") from exc

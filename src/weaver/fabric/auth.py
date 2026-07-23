"""Azure credentials for Fabric and OneLake.

``DefaultAzureCredential`` walks a chain of sources, and the one it settles on
is not always the one you are logged into. On a machine where ``az`` works and
ARM calls succeed, a OneLake write can still fail with
``401 Access token validation failed`` because the chain returned a token for a
different tenant.

The fix is to pin the chain. ``azure-identity`` 1.23 and later honour
``AZURE_TOKEN_CREDENTIALS``, so Weaver sets it to ``AzureCliCredential`` when
nothing else has, making the credential the same one ``az account show``
reports. Anyone who has deliberately chosen otherwise keeps their choice.
"""

from __future__ import annotations

import os

#: Scopes. Generic technical values, not environment-specific.
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
STORAGE_SCOPE = "https://storage.azure.com/.default"
SQL_SCOPE = "https://database.windows.net/.default"

#: Honoured by azure-identity >= 1.23 to pin DefaultAzureCredential's chain.
CREDENTIAL_ENV = "AZURE_TOKEN_CREDENTIALS"
DEFAULT_CREDENTIAL = "AzureCliCredential"


def prefer_cli_credential() -> str:
    """Pin the credential chain unless the caller already chose one."""

    existing = os.environ.get(CREDENTIAL_ENV)
    if existing:
        return existing
    os.environ[CREDENTIAL_ENV] = DEFAULT_CREDENTIAL
    return DEFAULT_CREDENTIAL


def credential():
    """A credential for Fabric, OneLake and SQL."""

    prefer_cli_credential()
    from azure.identity import DefaultAzureCredential

    return DefaultAzureCredential()


def get_token(scope: str, cred=None) -> str:
    """An access token for one scope."""

    return (cred or credential()).get_token(scope).token

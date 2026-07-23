"""Azure tokens for Fabric, OneLake and SQL.

Core does **not** decide which credential to use. It accepts an injected
credential and, absent one, falls back to ``DefaultAzureCredential`` — the
library default — without pinning the chain. Choosing a specific identity is a
caller's policy, not the core's.

That policy matters in practice: ``DefaultAzureCredential`` walks a chain and
does not always settle on the identity you are signed in as, so on a machine
where ``az`` works a OneLake write can still fail
``401 Access token validation failed``. ``azure-identity`` 1.23 honours
``AZURE_TOKEN_CREDENTIALS`` to pin the chain, and :func:`prefer_cli_credential`
sets it to ``AzureCliCredential`` — but a **caller** invokes that (the desktop
CLI does; the Fabric test infrastructure does). Core never sets it as a side
effect of asking for a token.
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
    """Pin the credential chain to the Azure CLI, unless already chosen.

    Policy, so a **caller** invokes it — the desktop CLI before a Fabric
    command, the test infrastructure before the Fabric suite. Core never calls
    it, so importing or using the core imposes no credential choice.
    """

    existing = os.environ.get(CREDENTIAL_ENV)
    if existing:
        return existing
    os.environ[CREDENTIAL_ENV] = DEFAULT_CREDENTIAL
    return DEFAULT_CREDENTIAL


def credential():
    """A default credential. Callers that want a specific one inject it instead."""

    from azure.identity import DefaultAzureCredential

    return DefaultAzureCredential()


def get_token(scope: str, cred=None) -> str:
    """An access token for one scope, from an injected credential or the default."""

    return (cred or credential()).get_token(scope).token

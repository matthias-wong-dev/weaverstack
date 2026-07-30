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
    """An access token for one scope, from an injected credential or the default.

    Answers the string and drops the expiry, which suits a one-shot command and
    nothing that outlives one. Anything long-lived wants :class:`TokenProvider`.
    """

    return (cred or credential()).get_token(scope).token


#: Renew this long before a token lapses, so a call already in flight when the
#: margin opens still carries a valid one.
TOKEN_REFRESH_MARGIN_SECONDS = 300.0


class TokenProvider:
    """A token for one scope, renewed shortly before it expires.

    This exists because holding the *string* is a bug that only shows up in long
    runs, and then shows up as something else. A caller that snapshots
    :func:`get_token` keeps sending the same bearer until the API starts
    answering ``401`` — and because ``AzureCliCredential`` serves from the Azure
    CLI's own cache, the string may already be most of the way through its life
    when it arrives. The usable budget is therefore *not* the nominal lifetime
    and cannot be assumed from it: a run that starts with a nearly-spent token
    has minutes, not an hour.

    That is not hypothetical. A Fabric suite whose session snapshotted its token
    died twenty minutes in with ``401: no body``, taking every downstream test
    with it, and looked like six unrelated failures.

    Refetching on every call would also be correct, and is what the SQL path does
    — but there a token is fetched per *connection*. A REST client fetches per
    *request*, and a credential shells out to ``az``, so the expiry is kept and
    the token renewed only when it is close.
    """

    def __init__(
        self,
        scope: str,
        cred=None,
        *,
        margin: float = TOKEN_REFRESH_MARGIN_SECONDS,
    ) -> None:
        self.scope = scope
        self._cred = cred
        self._margin = margin
        self._token: str | None = None
        self._expires_on = 0.0

    def _credential(self):
        # Built once and kept: constructing one per call would shell out to the
        # CLI every time, which is the cost this class exists to avoid.
        if self._cred is None:
            self._cred = credential()
        return self._cred

    def __call__(self) -> str:
        import time

        if self._token is None or time.time() >= self._expires_on - self._margin:
            acquired = self._credential().get_token(self.scope)
            self._token = acquired.token
            # A credential that reports no expiry gets renewed every call. Slow
            # rather than wrong, and no shipped credential does it.
            self._expires_on = float(getattr(acquired, "expires_on", 0) or 0)
        return self._token


def token_source(token=None, *, scope: str, cred=None):
    """Normalise what a caller supplied into a zero-argument token source.

    ``None`` builds a renewing :class:`TokenProvider`. A **string** is honoured
    exactly as given — the caller owns it and its lifetime, which is how a Fabric
    session passes on the identity it was handed. A **callable** is used as-is,
    so a caller with its own refresh keeps it.
    """

    if token is None:
        return TokenProvider(scope, cred)
    if callable(token):
        return token
    return lambda: token

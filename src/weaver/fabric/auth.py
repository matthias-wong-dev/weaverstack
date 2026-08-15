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

from ..errors import ConfigError

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


def checked_credential(supplied):
    """One injected credential, checked for the shape Azure's protocol names.

    Structural rather than an ``isinstance`` against ``TokenCredential``: the
    protocol is what matters and a caller may pass a wrapper, a fake, or a
    credential from a library Weaver does not import. What every one of them
    must have is a callable ``get_token``.

    Checked where it is *supplied* rather than where it is first used, because
    a Session acquires its token lazily — so a wrong object handed to
    ``weaver.session()`` would otherwise surface much later, during whichever
    operation happened to reach Fabric first.
    """

    if supplied is None:
        return None
    if not callable(getattr(supplied, "get_token", None)):
        raise ConfigError(
            "a credential must offer a callable get_token(*scopes), which is "
            f"the azure.core TokenCredential shape; {type(supplied).__name__} "
            "does not"
        )
    return supplied


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

    Holding the token *string* is a bug that only appears in long runs.
    ``AzureCliCredential`` serves from the Azure CLI's own cache, so an arriving
    token may already be most of the way through its life: the usable budget is
    not the nominal lifetime, and a snapshotted token starts answering ``401``
    part-way through a run.

    Refetching per call is also correct and is what the SQL path does, but there
    a token is fetched per connection. A REST client fetches per request and a
    credential shells out to ``az``, so the expiry is kept and the token renewed
    only when it is close.
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

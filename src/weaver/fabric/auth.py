"""Azure tokens for Fabric, OneLake and SQL.

Core does **not** decide which credential to use. It accepts an injected
credential and, absent one, falls back to the library default,
``DefaultAzureCredential``, without pinning the chain. Choosing a specific
identity is a caller's policy, not the core's.

That policy matters in practice: ``DefaultAzureCredential`` walks a chain and
does not always settle on the identity you are signed in as, so on a machine
where ``az`` works a OneLake write can still fail
``401 Access token validation failed``. ``azure-identity`` 1.23 honours
``AZURE_TOKEN_CREDENTIALS`` to pin the chain, and :func:`prefer_cli_credential`
sets it to ``AzureCliCredential``. The Fabric test infrastructure invokes that.
Core never sets it as a side effect of asking for a token.

A pinned chain names one credential type and cannot express a fallback, so the
desktop CLI installs an object instead: :func:`desktop_credential` answers the
Azure CLI where it works and Microsoft browser sign-in where it does not, and
:func:`use_credential` makes it this process's default. That reaches every
client, including the ones an operation constructs for itself.
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

#: Where browser sign-in keeps its tokens between commands. Without a name on
#: disk every Weaver command would open a browser window of its own.
TOKEN_CACHE_NAME = "weaverstack"


def prefer_cli_credential() -> str:
    """Pin the credential chain to the Azure CLI, unless already chosen.

    Policy, so a caller invokes it: the Fabric test infrastructure does, before
    its suite. Core never calls it, so importing or using the core imposes no
    credential choice.
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

    Checked where it is supplied rather than where it is first used, because
    a Session acquires its token lazily, so a wrong object handed to
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


#: The credential a caller installed for this process, or None for the library
#: default. Core never writes this; :func:`use_credential` is caller policy.
_installed = None


def use_credential(supplied):
    """Make one credential this process's default. Caller policy, not core's.

    An injected credential reaches only what receives it, and an operation
    constructs clients of its own, so a desktop policy that has to hold
    everywhere is installed here. Passing ``None`` restores the library default.
    """

    global _installed
    _installed = checked_credential(supplied)
    return _installed


def credential():
    """The installed credential, or the library default where none was installed."""

    if _installed is not None:
        return _installed

    from azure.identity import DefaultAzureCredential

    return DefaultAzureCredential()


#: The desktop chain, built at most once for this process.
_desktop_chain = None


def desktop_credential():
    """The Azure CLI where it can issue a token, and browser sign-in where it cannot.

    A desktop user who has run ``az login`` keeps that identity. One who has not
    is sent to Microsoft sign-in in a browser, and the token is cached on disk
    under :data:`TOKEN_CACHE_NAME`, so the next command signs in without opening
    anything.

    ``ChainedTokenCredential`` tries the Azure CLI inside the token acquisition a
    command was going to make anyway, moves on when it reports itself
    unavailable, and remembers which one answered. The Azure CLI is therefore
    asked once per process, and no command pays a round trip of its own to
    settle the policy.

    Built once per process, because the browser credential holds the token cache
    and a second one would sign in again.
    """

    global _desktop_chain
    if _desktop_chain is not None:
        return _desktop_chain

    from azure.identity import AzureCliCredential, ChainedTokenCredential

    _desktop_chain = ChainedTokenCredential(AzureCliCredential(), _browser_credential())
    return _desktop_chain


def _browser_credential():
    """Microsoft sign-in in a browser, remembered where the machine can keep it."""

    return BrowserSignIn()


class BrowserSignIn:
    """Microsoft sign-in in a browser, with the token cache made optional.

    The token is kept where the machine keeps secrets: a Keychain item on macOS,
    libsecret on Linux, DPAPI on Windows. ``azure-identity`` builds that cache
    when the first token is asked for, not when the credential is made, so the
    absence of secure storage surfaces here and is answered by signing in
    without a cache.

    Falling back to an unencrypted file is not offered. A refresh token sitting
    on disk in the clear is a worse trade than signing in again, and
    ``pip install weaverstack`` still has to be the whole prerequisite on a
    machine with no keyring at all.
    """

    def __init__(self) -> None:
        self._credential = None
        self._cached = True

    def get_token(self, *scopes, **kwargs):
        """One token, from the cached credential or from one without a cache."""

        from azure.core.exceptions import ClientAuthenticationError
        from azure.identity import CredentialUnavailableError

        credential = self._acquire()
        try:
            return credential.get_token(*scopes, **kwargs)
        except (ClientAuthenticationError, CredentialUnavailableError):
            # An answer about the sign-in itself, which is this credential's own
            # to report. Only the storage underneath it is worked around here.
            raise
        except Exception as exc:
            if not self._cached or not _is_cache_unavailable(exc):
                raise
            _warn_once(
                "This machine has no secure place to keep a sign-in "
                f"({type(exc).__name__}), so Weaver will ask you to sign in "
                "again next time."
            )
            self._credential = _interactive_browser(cached=False)
            self._cached = False
            return self._credential.get_token(*scopes, **kwargs)

    def _acquire(self):
        if self._credential is None:
            self._credential = _interactive_browser(cached=self._cached)
        return self._credential


#: What `azure-identity` says on Linux when it will not encrypt the cache and was
#: not allowed to write it in the clear. The message is the only signal: the
#: `ValueError` it raises carries the libsecret failure as its cause, and that
#: cause is an arbitrary platform exception.
_UNENCRYPTABLE = ("allow_unencrypted_storage", "Cache encryption is impossible")

#: `msal_extensions.build_encrypted_persistence` on a platform it has no store
#: for. Matched on the message, because the type it raises is `RuntimeError`.
_UNSUPPORTED = "Unsupported platform"

#: The library that holds the token, and the only missing import that says this
#: machine cannot keep one.
_EXTENSIONS = "msal_extensions"


def _is_cache_unavailable(exc: BaseException) -> bool:
    """Whether this failure is the token cache rather than the sign-in.

    Only the recognised ways a platform says it has nowhere secure to keep a
    token. Anything else is a defect somewhere else and propagates, because
    signing in a second time would neither fix it nor say so honestly.

    The cases, in the order a machine meets them:

    .. code-block:: text

        msal_extensions.PersistenceError   Keychain, DPAPI or libsecret refused
        ValueError naming the option       Linux with no usable libsecret
        NotImplementedError                a platform azure-identity has no store for
        RuntimeError naming the platform   the same, from msal_extensions
        ImportError for msal_extensions    the library holding the token is absent
    """

    if isinstance(exc, NotImplementedError):
        return True
    if _is_persistence_error(exc):
        return True
    message = str(exc)
    if isinstance(exc, ImportError):
        # Only the library that holds the token. Any other missing import is a
        # broken installation, and signing in again would not mend it.
        return getattr(exc, "name", None) == _EXTENSIONS or _EXTENSIONS in message
    if isinstance(exc, ValueError):
        return any(naming in message for naming in _UNENCRYPTABLE)
    if isinstance(exc, RuntimeError):
        return _UNSUPPORTED in message
    return False


def _is_persistence_error(exc: BaseException) -> bool:
    """Whether msal_extensions raised one of its own storage failures."""

    try:
        from msal_extensions.persistence import PersistenceError
    except ImportError:  # pragma: no cover - msal_extensions ships with the extra
        return False
    return isinstance(exc, PersistenceError)


def _interactive_browser(*, cached: bool):
    """The library's browser credential, with or without a persistent cache."""

    from azure.identity import (
        InteractiveBrowserCredential,
        TokenCachePersistenceOptions,
    )

    if not cached:
        return InteractiveBrowserCredential()
    return InteractiveBrowserCredential(
        cache_persistence_options=TokenCachePersistenceOptions(name=TOKEN_CACHE_NAME)
    )


#: Said once per process. A warning repeated per command is noise nobody reads.
_warned = False


def _warn_once(message: str) -> None:
    global _warned
    if _warned:
        return
    _warned = True
    import sys

    print(f"warning: {message}", file=sys.stderr)


def get_token(scope: str, cred=None) -> str:
    """An access token for one scope, from an injected credential or the default.

    Answers the string and drops the expiry, which suits a one-shot command and
    nothing that outlives one. Anything long-lived needs :class:`TokenProvider`.
    """

    return (cred or credential()).get_token(scope).token


#: Renew this long before a token lapses, so a call already in flight when the
#: margin opens still carries a valid one.
TOKEN_REFRESH_MARGIN_SECONDS = 300.0


class TokenProvider:
    """A token for one scope, renewed shortly before it expires.

    Holding the token string is a bug that only appears in long runs.
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
    exactly as given, so the caller owns it and its lifetime, which is how a Fabric
    session passes on the identity it was handed. A **callable** is used as-is,
    so a caller with its own refresh keeps it.
    """

    if token is None:
        return TokenProvider(scope, cred)
    if callable(token):
        return token
    return lambda: token

"""Azure tokens for Fabric, OneLake and SQL.

Credential selection belongs to the caller. Core accepts an injected credential
and otherwise uses ``DefaultAzureCredential`` without pinning its chain. The
desktop CLI installs a service principal, Azure CLI and browser sign-in chain;
``--non-interactive`` omits browser sign-in.
"""

from __future__ import annotations

import os
from pathlib import Path

from ..errors import ConfigError

FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
STORAGE_SCOPE = "https://storage.azure.com/.default"
SQL_SCOPE = "https://database.windows.net/.default"

# Honoured by azure-identity >= 1.23.
CREDENTIAL_ENV = "AZURE_TOKEN_CREDENTIALS"
DEFAULT_CREDENTIAL = "AzureCliCredential"

# The secure token cache shared between commands.
TOKEN_CACHE_NAME = "weaverstack"

WEAVER_DIRECTORY = ".weaver"

# Identity metadata only; tokens remain in the secure cache.
AUTHENTICATION_RECORD_FILE = "authentication-record.json"


def prefer_cli_credential() -> str:
    """Pin ``DefaultAzureCredential`` to the Azure CLI unless already pinned."""

    existing = os.environ.get(CREDENTIAL_ENV)
    if existing:
        return existing
    os.environ[CREDENTIAL_ENV] = DEFAULT_CREDENTIAL
    return DEFAULT_CREDENTIAL


def checked_credential(supplied):
    """Validate an injected credential structurally before lazy token acquisition."""

    if supplied is None:
        return None
    if not callable(getattr(supplied, "get_token", None)):
        raise ConfigError(
            "a credential must offer a callable get_token(*scopes), which is "
            f"the azure.core TokenCredential shape; {type(supplied).__name__} "
            "does not"
        )
    return supplied


_installed = None


def use_credential(supplied):
    """Install a process-wide default; ``None`` restores the library default."""

    global _installed
    _installed = checked_credential(supplied)
    return _installed


def credential():
    if _installed is not None:
        return _installed

    from azure.identity import DefaultAzureCredential

    return DefaultAzureCredential()


_desktop_chain = None

_unattended_chain = None


def desktop_credential():
    """Return the process-wide service principal, Azure CLI and browser chain.

    ``EnvironmentCredential`` reads the standard service-principal variables.
    Browser sign-in reuses the platform's secure token cache when available.
    """

    global _desktop_chain
    if _desktop_chain is not None:
        return _desktop_chain

    from azure.identity import AzureCliCredential, ChainedTokenCredential

    diagnostic = {}
    credentials = [
        DiagnosticCredential(_principal_credential(), "Service principal", diagnostic)
    ]
    credentials.append(
        DiagnosticCredential(AzureCliCredential(), "Azure CLI", diagnostic)
    )
    credentials.append(
        DiagnosticCredential(_browser_credential(), "Browser sign-in", diagnostic)
    )
    _desktop_chain = ChainedTokenCredential(*credentials)
    _desktop_chain.diagnostic = diagnostic
    return _desktop_chain


def unattended_credential():
    """Return the process-wide service principal and Azure CLI chain."""

    global _unattended_chain
    if _unattended_chain is not None:
        return _unattended_chain

    from azure.identity import AzureCliCredential, ChainedTokenCredential

    diagnostic = {}
    _unattended_chain = ChainedTokenCredential(
        DiagnosticCredential(_principal_credential(), "Service principal", diagnostic),
        DiagnosticCredential(AzureCliCredential(), "Azure CLI", diagnostic),
    )
    _unattended_chain.diagnostic = diagnostic
    return _unattended_chain


def _principal_configured() -> bool:
    return bool(
        os.environ.get("AZURE_CLIENT_ID")
        and os.environ.get("AZURE_TENANT_ID")
        and (
            os.environ.get("AZURE_CLIENT_SECRET")
            or os.environ.get("AZURE_CLIENT_CERTIFICATE_PATH")
        )
    )


def _principal_credential():
    """Defer service-principal configuration until the chain requests a token."""

    return _PrincipalCredential()


class _PrincipalCredential:
    def __init__(self) -> None:
        self._credential = None

    def get_token(self, *scopes, **kwargs):
        from azure.identity import CredentialUnavailableError

        if not _principal_configured():
            raise CredentialUnavailableError(
                "no service principal is configured "
                "(set AZURE_CLIENT_ID and AZURE_TENANT_ID, plus either "
                "AZURE_CLIENT_SECRET or AZURE_CLIENT_CERTIFICATE_PATH)"
            )
        if self._credential is None:
            from azure.identity import EnvironmentCredential

            self._credential = EnvironmentCredential()
        return self._credential.get_token(*scopes, **kwargs)

    def close(self):
        close = getattr(self._credential, "close", None)
        if close is not None:
            close()


class DiagnosticCredential:
    """Record the successful credential path without retaining token contents."""

    def __init__(self, wrapped, name, diagnostic):
        self.wrapped = wrapped
        self.name = name
        self.diagnostic = diagnostic

    def get_token(self, *scopes, **kwargs):
        token = self.wrapped.get_token(*scopes, **kwargs)
        self.diagnostic.clear()
        self.diagnostic["path"] = self.name
        identity = getattr(self.wrapped, "identity", {})
        self.diagnostic.update(
            {key: identity[key] for key in ("account", "tenant") if identity.get(key)}
        )
        return token

    def close(self):
        close = getattr(self.wrapped, "close", None)
        if close is not None:
            close()


def _browser_credential():
    return BrowserSignIn()


# The record is convenience state. Every failure below opens the browser instead.


def _authentication_record_path() -> Path:
    return Path.home() / WEAVER_DIRECTORY / AUTHENTICATION_RECORD_FILE


def _load_authentication_record():
    from azure.identity import AuthenticationRecord

    path = _authentication_record_path()
    try:
        serialised = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError as exc:
        _warn_once(f"Weaver could not read {path} ({type(exc).__name__}).")
        return None
    try:
        return AuthenticationRecord.deserialize(serialised)
    except Exception as exc:  # noqa: BLE001 - an unusable record is replaced
        _warn_once(
            f"The sign-in Weaver remembered in {path} could not be read "
            f"({type(exc).__name__}), so it will ask you to sign in again."
        )
        return None


def _save_authentication_record(record) -> None:
    path = _authentication_record_path()
    temporary = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary.write_text(record.serialize(), encoding="utf-8")
        _restrict(temporary)
        os.replace(temporary, path)
    except OSError as exc:
        _warn_once(
            f"Weaver could not remember this sign-in in {path} "
            f"({type(exc).__name__}), so it will ask you to sign in again."
        )
        try:
            temporary.unlink()
        except OSError:
            pass


def _restrict(path: Path) -> None:
    # Windows does not expose this mode through chmod.
    try:
        os.chmod(path, 0o600)
    except (OSError, NotImplementedError):
        pass


class BrowserSignIn:
    """Microsoft sign-in in a browser, reused across commands.

    Cross-process reuse needs both the platform's encrypted token cache and its
    ``AuthenticationRecord``. Weaver stores the record, but never requests an
    unencrypted token cache. Without secure storage, each command signs in.
    """

    def __init__(self) -> None:
        self._credential = None
        self._cached = True
        self.identity = {}

    def get_token(self, *scopes, **kwargs):
        from azure.identity import AuthenticationRequiredError

        try:
            return self._through_the_cache(
                lambda credential: credential.get_token(*scopes, **kwargs)
            )
        except AuthenticationRequiredError as required:
            return self._sign_in(required, **kwargs)

    def _sign_in(self, required, **kwargs):
        arguments = _authenticate_arguments(required, kwargs)

        def authenticate(credential):
            record = credential.authenticate(**arguments)
            self._record_identity(record)
            token = credential.get_token(*required.scopes, **kwargs)
            # After the token: this call is where a lazy cache can still report
            # itself unavailable, and the record is only usable beside the cache
            # holding the refresh token.
            if self._cached:
                _save_authentication_record(record)
            return token

        return self._through_the_cache(authenticate)

    def _through_the_cache(self, use):
        """Retry without caching only when secure token storage is unavailable."""

        from azure.core.exceptions import ClientAuthenticationError
        from azure.identity import CredentialUnavailableError

        try:
            return use(self._acquire())
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
            self._cached = False
            self._credential = _interactive_browser(cached=False, record=None)
            return use(self._credential)

    def _acquire(self):
        if self._credential is None:
            record = _load_authentication_record() if self._cached else None
            self._record_identity(record)
            self._credential = _interactive_browser(cached=self._cached, record=record)
        return self._credential

    def _record_identity(self, record):
        self.identity = {
            "account": getattr(record, "username", None),
            "tenant": getattr(record, "tenant_id", None),
        }


def _authenticate_arguments(required, kwargs) -> dict:
    arguments = {"scopes": list(required.scopes)}
    claims = getattr(required, "claims", None)
    if claims:
        arguments["claims"] = claims
    tenant = kwargs.get("tenant_id")
    if tenant:
        arguments["tenant_id"] = tenant
    if kwargs.get("enable_cae"):
        arguments["enable_cae"] = True
    return arguments


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
    """Recognise secure-storage failures without hiding sign-in failures."""

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
    try:
        from msal_extensions.persistence import PersistenceError
    except ImportError:  # pragma: no cover - msal_extensions ships with the extra
        return False
    return isinstance(exc, PersistenceError)


def _interactive_browser(*, cached: bool, record=None):
    from azure.identity import (
        InteractiveBrowserCredential,
        TokenCachePersistenceOptions,
    )

    if not cached:
        return InteractiveBrowserCredential(disable_automatic_authentication=True)
    return InteractiveBrowserCredential(
        authentication_record=record,
        cache_persistence_options=TokenCachePersistenceOptions(name=TOKEN_CACHE_NAME),
        disable_automatic_authentication=True,
    )


_warned: set = set()


def _warn_once(message: str) -> None:
    if message in _warned:
        return
    _warned.add(message)
    import sys

    print(f"warning: {message}", file=sys.stderr)


def get_token(scope: str, cred=None) -> str:
    """Return a token string for one-shot use, without expiry metadata."""

    return (cred or credential()).get_token(scope).token


#: Renew this long before a token lapses, so a call already in flight when the
#: margin opens still carries a valid one.
TOKEN_REFRESH_MARGIN_SECONDS = 300.0


class TokenProvider:
    """A token for one scope, renewed shortly before it expires.

    A REST client calls this per request. Caching avoids invoking the Azure CLI
    each time while expiry tracking prevents a long run from retaining a stale
    token.
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

    @property
    def diagnostic(self) -> dict:
        credential = self._credential()
        return dict(
            getattr(credential, "diagnostic", {"path": type(credential).__name__})
        )

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
    """Return a renewing provider, a caller's callable or its fixed token string."""

    if token is None:
        return TokenProvider(scope, cred)
    if callable(token):
        return token
    return lambda: token

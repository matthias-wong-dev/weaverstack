"""How a desktop command signs in, and what that costs when it is already signed in.

`pip install weaverstack` is the whole prerequisite. A user who has run
`az login` keeps that identity; one who has not is sent to Microsoft sign-in in
a browser once, and later commands reuse it.

The choice is a chain, not a probe. `ChainedTokenCredential` tries the Azure CLI
inside the token acquisition a command was going to make anyway and remembers
which credential answered, so a signed-in user pays nothing to have a fallback
available.

The choice is the CLI's. Core accepts an installed credential and never installs
one, and the Fabric suite stays explicitly Azure-CLI based, so an unattended run
can never be sent to a browser.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.errors import ConfigError
from weaver.fabric import auth

#: The real resolver, captured before the suite-wide guard replaces it. Nothing
#: here asks Azure for anything: the library default is replaced too, so what is
#: exercised is the choice rather than a credential.
REAL_CREDENTIAL = auth.credential


class _Token:
    token = "a-token"
    expires_on = 4102444800


class _Working:
    """A credential that answers."""

    def __init__(self):
        self.calls = 0

    def get_token(self, *scopes, **_):
        self.calls += 1
        return _Token()


class _Unavailable:
    """A credential that reports it cannot be used here."""

    def __init__(self):
        self.calls = 0

    def get_token(self, *scopes, **_):
        from azure.identity import CredentialUnavailableError

        self.calls += 1
        raise CredentialUnavailableError("Please run 'az login'")


class _LibraryDefault:
    """Stands in for ``DefaultAzureCredential``, which is never built here."""

    def get_token(self, *scopes, **_):
        return _Token()


@pytest.fixture(autouse=True)
def uninstalled(monkeypatch, tmp_path):
    """Leave the process as this test found it, and reach no real credential.

    The suite refuses a real credential outside `-m fabric`. These tests are
    about which credential is chosen, so the resolver is restored and the
    library default replaced, and nothing asks Azure anything.
    """

    monkeypatch.setattr(auth, "credential", REAL_CREDENTIAL)
    monkeypatch.setattr("azure.identity.DefaultAzureCredential", _LibraryDefault)
    # Nothing here may read or write the developer's own remembered sign-in.
    monkeypatch.setattr(
        auth, "_authentication_record_path", lambda: tmp_path / "record.json"
    )
    monkeypatch.setattr(auth, "_warned", set())

    before = auth._installed
    chain = auth._desktop_chain
    yield
    auth._installed = before
    auth._desktop_chain = chain


@pytest.fixture
def credentials(monkeypatch):
    """The two halves of the chain, each recording what was asked of it."""

    cli = _Working()
    browser = _Working()
    auth._desktop_chain = None
    monkeypatch.setattr("azure.identity.AzureCliCredential", lambda *a, **k: cli)
    monkeypatch.setattr(auth, "_browser_credential", lambda: browser)
    return type("Credentials", (), {"cli": cli, "browser": browser})


# --- installing a choice -------------------------------------------------------


@weaver_test()
def test_core_installs_nothing_of_its_own():
    """Importing Weaver imposes no credential on the process that imported it.

    In a process of its own, because what is claimed is a property of the import
    and this one has already run tests that install one.
    """

    import subprocess
    import sys

    probe = (
        "import weaver, weaver.fabric.auth as auth;"
        "print(auth._installed, auth._desktop_chain)"
    )
    answered = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, check=True
    )

    assert answered.stdout.strip() == "None None"


@weaver_test()
def test_an_installed_credential_is_what_core_then_uses():
    supplied = _Working()

    auth.use_credential(supplied)

    assert auth.credential() is supplied


@weaver_test()
def test_installing_nothing_restores_the_library_default():
    auth.use_credential(_Working())
    auth.use_credential(None)

    assert isinstance(auth.credential(), _LibraryDefault)


@weaver_test()
def test_an_object_that_is_not_a_credential_is_refused_here():
    with pytest.raises(ConfigError, match="get_token"):
        auth.use_credential(object())


# --- choosing between the two --------------------------------------------------


@weaver_test()
def test_the_azure_cli_answers_where_it_can(credentials):
    chosen = auth.desktop_credential()

    assert chosen.get_token(auth.FABRIC_SCOPE).token == "a-token"
    assert credentials.cli.calls == 1
    assert credentials.browser.calls == 0


@weaver_test()
def test_the_browser_answers_where_the_azure_cli_cannot(monkeypatch):
    unavailable = _Unavailable()
    browser = _Working()
    auth._desktop_chain = None
    monkeypatch.setattr(
        "azure.identity.AzureCliCredential", lambda *a, **k: unavailable
    )
    monkeypatch.setattr(auth, "_browser_credential", lambda: browser)

    token = auth.desktop_credential().get_token(auth.FABRIC_SCOPE)

    assert token.token == "a-token"
    assert unavailable.calls == 1
    assert browser.calls == 1


@weaver_test()
def test_neither_answering_raises_rather_than_returning_nothing(monkeypatch):
    auth._desktop_chain = None
    monkeypatch.setattr(
        "azure.identity.AzureCliCredential", lambda *a, **k: _Unavailable()
    )
    monkeypatch.setattr(auth, "_browser_credential", _Unavailable)

    from azure.core.exceptions import ClientAuthenticationError

    with pytest.raises(ClientAuthenticationError):
        auth.desktop_credential().get_token(auth.FABRIC_SCOPE)


@weaver_test()
def test_the_chain_is_built_once_for_the_process(credentials):
    """The browser credential holds the token cache, and a second one signs in again."""

    first = auth.desktop_credential()
    second = auth.desktop_credential()

    assert first is second


@weaver_test()
def test_a_signed_in_user_is_never_sent_to_a_browser(credentials):
    """Several commands in one process ask the Azure CLI and stop there."""

    chosen = auth.desktop_credential()
    for _ in range(3):
        chosen.get_token(auth.FABRIC_SCOPE)

    assert credentials.browser.calls == 0


# --- what the CLI does with it -------------------------------------------------


@weaver_test()
def test_the_desktop_cli_installs_the_chain(credentials):
    from weaver_cli.main import _prefer_desktop_credential

    _prefer_desktop_credential()

    assert auth.credential() is auth.desktop_credential()


# --- what the Fabric suite does ------------------------------------------------


@weaver_test()
def test_pinning_the_chain_to_the_azure_cli_is_still_available(monkeypatch):
    """Caller policy, so it stays offered. Core never calls it."""

    monkeypatch.delenv(auth.CREDENTIAL_ENV, raising=False)

    assert auth.prefer_cli_credential() == "AzureCliCredential"

    import os

    assert os.environ[auth.CREDENTIAL_ENV] == "AzureCliCredential"


@weaver_test()
def test_the_fabric_suite_installs_the_desktop_chain():
    """The same chain a `weaver` command uses, so a browser sign-in the user
    already performed runs the suite."""

    from pathlib import Path

    conftest = Path(__file__).resolve().parent / "fabric" / "conftest.py"
    source = conftest.read_text(encoding="utf-8")

    assert "use_credential(desktop_credential())" in source


@weaver_test()
def test_an_unreachable_estate_fails_a_run_that_asked_for_fabric():
    from pathlib import Path

    conftest = Path(__file__).resolve().parent / "fabric" / "conftest.py"
    source = conftest.read_text(encoding="utf-8")

    assert "if _asked_for_fabric(request.config):" in source
    assert "pytest.fail(unreachable, pytrace=False)" in source


# --- signing in once, and not again -------------------------------------------


def _Record(username: str):
    """A real record, so the serialise and deserialise round trip is exercised."""

    from azure.identity import AuthenticationRecord

    return AuthenticationRecord(
        "a-tenant",
        "a-client",
        "login.microsoftonline.com",
        f"home-{username}",
        username,
    )


class _Browser:
    """The library's browser credential under `disable_automatic_authentication`:
    silent with a record Entra will renew, `AuthenticationRequiredError` without
    one."""

    def __init__(self, record, *, signs_in_as, renews, cache_fails_late=False):
        self.record = record
        self.authenticated = []
        self.tokens = 0
        self._signs_in_as = signs_in_as
        self._renews = renews
        #: The cache is built lazily, so it can be this call that finds it gone.
        self._cache_fails_late = cache_fails_late

    def get_token(self, *scopes, **kwargs):
        from azure.identity import AuthenticationRequiredError

        if self._cache_fails_late and self.authenticated:
            raise UNENCRYPTABLE
        if self.record is None or not self._renews:
            raise AuthenticationRequiredError(
                scopes=list(scopes), claims=kwargs.get("claims")
            )
        self.tokens += 1
        return _Token()

    def authenticate(self, **kwargs):
        self.authenticated.append(kwargs)
        self.record = _Record(self._signs_in_as)
        # Interactive sign-in is what Entra asked for, and it renews from here.
        self._renews = True
        return self.record


class _Browsers(list):
    """Every browser credential built, and what the next one will do."""

    renews = True
    signs_in_as = "someone@example.com"
    cache_fails_late = False

    def build(self, *, cached: bool, record=None):
        made = _Browser(
            record,
            signs_in_as=self.signs_in_as,
            renews=self.renews,
            cache_fails_late=cached and self.cache_fails_late,
        )
        made.cached = cached
        self.append(made)
        return made


@pytest.fixture
def browsers(monkeypatch):
    built = _Browsers()
    monkeypatch.setattr(auth, "_interactive_browser", built.build)
    return built


@weaver_test()
def test_a_first_sign_in_opens_a_browser_and_keeps_the_account(browsers):
    token = auth.BrowserSignIn().get_token(auth.FABRIC_SCOPE)

    assert token.token == "a-token"
    assert len(browsers[0].authenticated) == 1
    assert auth._load_authentication_record().username == "someone@example.com"


@weaver_test()
def test_a_later_process_reuses_the_sign_in_and_opens_nothing(browsers):
    """A fresh `BrowserSignIn` stands in for a new process."""

    auth.BrowserSignIn().get_token(auth.FABRIC_SCOPE)

    later = auth.BrowserSignIn().get_token(auth.FABRIC_SCOPE)

    assert later.token == "a-token"
    assert browsers[1].record is not None, "the credential was built without an account"
    assert browsers[1].tokens == 1
    assert browsers[1].authenticated == [], "a browser opened in the second process"


@weaver_test()
def test_a_sign_in_entra_will_not_renew_reaches_the_browser_once(browsers):
    auth._save_authentication_record(_Record("someone@example.com"))
    browsers.renews = False

    token = auth.BrowserSignIn().get_token(auth.FABRIC_SCOPE)

    assert token.token == "a-token"
    assert len(browsers) == 1, "the credential was rebuilt rather than authenticated"
    assert len(browsers[0].authenticated) == 1
    assert browsers[0].tokens == 1


@weaver_test()
def test_signing_in_as_another_account_replaces_the_record(browsers):
    auth._save_authentication_record(_Record("first@example.com"))
    browsers.renews = False
    browsers.signs_in_as = "second@example.com"

    auth.BrowserSignIn().get_token(auth.FABRIC_SCOPE)

    assert auth._load_authentication_record().username == "second@example.com"


@weaver_test()
def test_a_cache_lost_after_authenticating_remembers_nothing(browsers, capsys):
    """The record is written after the cached token, so the fallback leaves none.

    `authenticate` succeeds, and the `get_token` behind it is where the platform
    reports it has nowhere to keep a token. The uncached credential then answers,
    and a record written before that would name an account with no cached refresh
    token behind it.
    """

    browsers.cache_fails_late = True

    token = auth.BrowserSignIn().get_token(auth.FABRIC_SCOPE)

    assert token.token == "a-token"
    assert [made.cached for made in browsers] == [True, False]
    assert len(browsers[1].authenticated) == 1
    assert not auth._authentication_record_path().exists()
    assert "no secure place to keep a sign-in" in capsys.readouterr().err


@weaver_test()
def test_the_refused_request_is_what_the_sign_in_asks_for(browsers):
    browsers.renews = False
    claims = '{"access_token":{"nbf":{"essential":true}}}'

    auth.BrowserSignIn().get_token(
        auth.FABRIC_SCOPE, claims=claims, tenant_id="a-tenant", enable_cae=True
    )

    asked = browsers[0].authenticated[0]
    assert asked["scopes"] == [auth.FABRIC_SCOPE]
    assert asked["claims"] == claims
    assert asked["tenant_id"] == "a-tenant"
    assert asked["enable_cae"] is True


@weaver_test()
def test_a_record_that_cannot_be_read_is_replaced(browsers, capsys):
    path = auth._authentication_record_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{ this is not a record", encoding="utf-8")

    token = auth.BrowserSignIn().get_token(auth.FABRIC_SCOPE)

    assert token.token == "a-token"
    assert len(browsers[0].authenticated) == 1
    assert auth._load_authentication_record().username == "someone@example.com"
    assert "could not be read" in capsys.readouterr().err


@weaver_test()
def test_a_record_that_cannot_be_written_costs_the_reuse_and_not_the_token(
    browsers, monkeypatch, capsys
):
    def refuse(*arguments, **keywords):
        raise OSError("read-only file system")

    monkeypatch.setattr(auth.Path, "mkdir", refuse)

    token = auth.BrowserSignIn().get_token(auth.FABRIC_SCOPE)

    assert token.token == "a-token"
    assert "could not remember this sign-in" in capsys.readouterr().err


@weaver_test()
def test_a_half_written_record_is_never_left_behind(monkeypatch):
    replaced = []
    monkeypatch.setattr(
        auth.os, "replace", lambda source, into: replaced.append(source)
    )

    auth._save_authentication_record(_Record("someone@example.com"))

    written = replaced[0]
    assert str(written) != str(auth._authentication_record_path())
    assert not auth._authentication_record_path().exists()


@weaver_test()
def test_the_record_is_owner_only_where_the_platform_has_such_a_mode():
    import os
    import sys

    auth._save_authentication_record(_Record("someone@example.com"))

    if sys.platform == "win32":  # pragma: no cover - the mode has no meaning there
        return
    mode = os.stat(auth._authentication_record_path()).st_mode & 0o777
    assert mode == 0o600


@weaver_test()
def test_the_record_carries_no_token(browsers):
    auth.BrowserSignIn().get_token(auth.FABRIC_SCOPE)

    kept = auth._authentication_record_path().read_text(encoding="utf-8")
    assert "a-token" not in kept
    assert "refresh" not in kept.casefold()
    assert "secret" not in kept.casefold()


@weaver_test()
def test_a_signed_in_user_never_reads_the_remembered_account(credentials, monkeypatch):
    def refuse():
        raise AssertionError("the remembered account was read for a CLI sign-in")

    monkeypatch.setattr(auth, "_load_authentication_record", refuse)

    auth.desktop_credential().get_token(auth.FABRIC_SCOPE)

    assert credentials.cli.calls == 1
    assert credentials.browser.calls == 0


# --- a machine with nowhere to keep a sign-in ----------------------------------
#
# The token is kept where the platform keeps secrets. A headless Linux box with
# no keyring has none, and `pip install weaverstack` still has to be the whole
# prerequisite there, so the sign-in works without a cache and says so once.


#: What azure-identity raises on a Linux box with no usable libsecret, in the
#: words it uses. The message is the signal: the ValueError carries the
#: platform's own failure as its cause, and that could be anything.
UNENCRYPTABLE = ValueError(
    "Cache encryption is impossible because libsecret dependencies are not "
    'installed or are unusable. Specify "allow_unencrypted_storage=True" to '
    "store the cache unencrypted instead of raising this exception."
)


class _NoSecureStorage:
    """The browser credential azure-identity builds when a cache was asked for.

    The cache is built when the first token is asked for, not when the
    credential is made, so this is where the absence of secure storage lands.
    """

    def __init__(self, raising=None):
        self.calls = 0
        self._raising = raising if raising is not None else UNENCRYPTABLE

    def get_token(self, *scopes, **_):
        self.calls += 1
        raise self._raising


@pytest.fixture
def browser(monkeypatch):
    """The library's browser credential, cached and uncached, each recorded."""

    made: list[bool] = []
    with_a_cache = _NoSecureStorage()
    without_one = _Working()

    def build(*, cached: bool, record=None):
        made.append(cached)
        return with_a_cache if cached else without_one

    monkeypatch.setattr(auth, "_interactive_browser", build)
    monkeypatch.setattr(auth, "_warned", set())
    return type(
        "Browser",
        (),
        {"made": made, "cached": with_a_cache, "uncached": without_one},
    )


@weaver_test()
def test_a_machine_with_no_secure_storage_still_signs_in(browser, capsys):
    token = auth.BrowserSignIn().get_token(auth.FABRIC_SCOPE)

    assert token.token == "a-token"
    assert browser.made == [True, False]
    assert browser.cached.calls == 1
    assert browser.uncached.calls == 1
    assert "no secure place to keep a sign-in" in capsys.readouterr().err


@weaver_test()
def test_a_machine_with_no_secure_storage_remembers_nothing(browser):
    """A record with no cached refresh token behind it reuses nothing."""

    auth.BrowserSignIn().get_token(auth.FABRIC_SCOPE)

    assert not auth._authentication_record_path().exists()


@weaver_test()
def test_the_uncached_warning_is_said_once(browser, capsys):
    sign_in = auth.BrowserSignIn()
    sign_in.get_token(auth.FABRIC_SCOPE)
    sign_in.get_token(auth.FABRIC_SCOPE)

    assert capsys.readouterr().err.count("no secure place") == 1


@weaver_test()
def test_the_cache_is_dropped_once_and_not_probed_again(browser):
    sign_in = auth.BrowserSignIn()
    sign_in.get_token(auth.FABRIC_SCOPE)
    sign_in.get_token(auth.FABRIC_SCOPE)

    assert browser.made == [True, False]
    assert browser.cached.calls == 1


@weaver_test()
def test_a_secure_machine_keeps_the_cache(monkeypatch, capsys):
    """Nothing is worked around where the platform can hold a token."""

    made: list[bool] = []
    working = _Working()

    def build(*, cached: bool, record=None):
        made.append(cached)
        return working

    monkeypatch.setattr(auth, "_interactive_browser", build)

    auth.BrowserSignIn().get_token(auth.FABRIC_SCOPE)

    assert made == [True]
    assert capsys.readouterr().err == ""


@weaver_test()
def test_a_refused_sign_in_is_reported_rather_than_retried(monkeypatch):
    """An answer about the sign-in is not a reason to sign in again."""

    from azure.core.exceptions import ClientAuthenticationError

    made: list[bool] = []

    class _Refused:
        def get_token(self, *scopes, **_):
            raise ClientAuthenticationError("the user cancelled")

    def build(*, cached: bool, record=None):
        made.append(cached)
        return _Refused()

    monkeypatch.setattr(auth, "_interactive_browser", build)

    with pytest.raises(ClientAuthenticationError):
        auth.BrowserSignIn().get_token(auth.FABRIC_SCOPE)

    assert made == [True]


@weaver_test()
def test_an_unencrypted_cache_is_never_asked_for():
    """A refresh token in the clear is a worse trade than signing in again.

    The option is named in this module, in the message azure-identity uses to
    say it will not write one. What must never appear is the request for it.
    """

    from pathlib import Path

    source = (Path(auth.__file__).resolve()).read_text(encoding="utf-8")

    assert "allow_unencrypted_storage=True" not in source
    assert "allow_unencrypted_storage=" not in source


# --- only the cache is worked around -------------------------------------------
#
# A second browser window is a real cost, and a warning about secure storage is
# a wrong answer for a defect somewhere else. So the fallback recognises the
# ways a platform says it has nowhere to keep a token, and nothing else.


@pytest.mark.parametrize(
    "raised",
    [
        UNENCRYPTABLE,
        NotImplementedError("A persistent cache is not available in this environment."),
        RuntimeError("Unsupported platform: sunos"),
        ImportError("No module named 'msal_extensions'", name="msal_extensions"),
    ],
    ids=["no-libsecret", "no-store", "unsupported-platform", "no-msal-extensions"],
)
@weaver_test()
def test_each_way_a_platform_says_it_cannot_keep_a_token_falls_back(
    monkeypatch, capsys, raised
):
    made: list[bool] = []
    refusing = _NoSecureStorage(raising=raised)
    working = _Working()

    def build(*, cached: bool, record=None):
        made.append(cached)
        return refusing if cached else working

    monkeypatch.setattr(auth, "_interactive_browser", build)
    monkeypatch.setattr(auth, "_warned", set())

    assert auth.BrowserSignIn().get_token(auth.FABRIC_SCOPE).token == "a-token"
    assert made == [True, False]
    assert "no secure place to keep a sign-in" in capsys.readouterr().err


@weaver_test()
def test_a_persistence_error_is_recognised_by_its_type(monkeypatch, capsys):
    """msal_extensions raises its own type, whatever the platform said."""

    from msal_extensions.persistence import PersistenceEncryptionError

    made: list[bool] = []
    refusing = _NoSecureStorage(raising=PersistenceEncryptionError(message="keychain"))
    working = _Working()

    def build(*, cached: bool, record=None):
        made.append(cached)
        return refusing if cached else working

    monkeypatch.setattr(auth, "_interactive_browser", build)
    monkeypatch.setattr(auth, "_warned", set())

    auth.BrowserSignIn().get_token(auth.FABRIC_SCOPE)

    assert made == [True, False]


@weaver_test()
def test_an_unrelated_failure_propagates_and_opens_no_second_browser(
    monkeypatch, capsys
):
    """The inverse claim: a defect elsewhere is not read as a missing keyring."""

    made: list[bool] = []
    broken = _NoSecureStorage(raising=RuntimeError("something else went wrong"))

    def build(*, cached: bool, record=None):
        made.append(cached)
        return broken

    monkeypatch.setattr(auth, "_interactive_browser", build)
    monkeypatch.setattr(auth, "_warned", set())

    with pytest.raises(RuntimeError, match="something else went wrong"):
        auth.BrowserSignIn().get_token(auth.FABRIC_SCOPE)

    assert made == [True]
    assert broken.calls == 1
    assert "no secure place" not in capsys.readouterr().err


@weaver_test()
def test_a_bare_value_error_is_not_a_missing_keyring(monkeypatch, capsys):
    """The message is the signal, so a ValueError from elsewhere still propagates."""

    made: list[bool] = []
    broken = _NoSecureStorage(raising=ValueError("some other argument was wrong"))

    monkeypatch.setattr(
        auth,
        "_interactive_browser",
        lambda *, cached, record=None: made.append(cached) or broken,
    )
    monkeypatch.setattr(auth, "_warned", set())

    with pytest.raises(ValueError, match="some other argument"):
        auth.BrowserSignIn().get_token(auth.FABRIC_SCOPE)

    assert made == [True]
    assert "no secure place" not in capsys.readouterr().err


@weaver_test()
def test_an_unrelated_missing_import_is_not_a_missing_keyring(monkeypatch, capsys):
    """A broken installation is not mended by signing in again."""

    made: list[bool] = []
    broken = _NoSecureStorage(
        raising=ImportError("No module named 'something_else'", name="something_else")
    )

    monkeypatch.setattr(
        auth,
        "_interactive_browser",
        lambda *, cached, record=None: made.append(cached) or broken,
    )
    monkeypatch.setattr(auth, "_warned", set())

    with pytest.raises(ImportError, match="something_else"):
        auth.BrowserSignIn().get_token(auth.FABRIC_SCOPE)

    assert made == [True]
    assert broken.calls == 1
    assert "no secure place" not in capsys.readouterr().err


@weaver_test()
def test_the_library_that_holds_the_token_is_recognised_by_name(monkeypatch, capsys):
    """`ImportError.name` where Python set it, and the message where it did not."""

    for raised in (
        ImportError("No module named 'msal_extensions'", name="msal_extensions"),
        ImportError("cannot import name 'PersistedTokenCache' from msal_extensions"),
    ):
        made: list[bool] = []
        working = _Working()
        refusing = _NoSecureStorage(raising=raised)

        monkeypatch.setattr(
            auth,
            "_interactive_browser",
            lambda *, cached, record=None: (
                made.append(cached) or (refusing if cached else working)
            ),
        )
        monkeypatch.setattr(auth, "_warned", set())

        assert auth.BrowserSignIn().get_token(auth.FABRIC_SCOPE).token == "a-token"
        assert made == [True, False]


@weaver_test()
def test_authentication_diagnostic_names_the_successful_path(credentials):
    provider = auth.TokenProvider(auth.FABRIC_SCOPE, auth.desktop_credential())
    provider()
    assert provider.diagnostic == {"path": "Azure CLI"}
    assert "a-token" not in str(provider.diagnostic)


@weaver_test()
def test_authentication_diagnostic_reports_browser_fallback(monkeypatch):
    auth._desktop_chain = None
    monkeypatch.setattr("azure.identity.AzureCliCredential", lambda: _Unavailable())
    monkeypatch.setattr(auth, "_browser_credential", _Working)
    provider = auth.TokenProvider(auth.FABRIC_SCOPE, auth.desktop_credential())
    provider()
    assert provider.diagnostic == {"path": "Browser sign-in"}

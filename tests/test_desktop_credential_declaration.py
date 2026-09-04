"""How a desktop command signs in, and what that costs when it is already signed in.

`pip install weaverstack` is the whole prerequisite. A user who has run
`az login` keeps that identity; one who has not is sent to Microsoft sign-in in
a browser, and the token is cached on disk so the next command opens nothing.

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
def uninstalled(monkeypatch):
    """Leave the process as this test found it, and reach no real credential.

    The suite refuses a real credential outside `-m fabric`. These tests are
    about which credential is chosen, so the resolver is restored and the
    library default replaced, and nothing asks Azure anything.
    """

    monkeypatch.setattr(auth, "credential", REAL_CREDENTIAL)
    monkeypatch.setattr("azure.identity.DefaultAzureCredential", _LibraryDefault)

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
def test_the_fabric_suite_stays_pinned_to_the_azure_cli(monkeypatch):
    """An unattended run must never be able to open a browser."""

    monkeypatch.delenv(auth.CREDENTIAL_ENV, raising=False)

    assert auth.prefer_cli_credential() == "AzureCliCredential"

    import os

    assert os.environ[auth.CREDENTIAL_ENV] == "AzureCliCredential"


@weaver_test()
def test_the_fabric_conftest_asks_for_the_azure_cli_and_nothing_else():
    """Source-level, because what it guards is a line nobody meant to change.

    A fixture that reached for the desktop policy would put an interactive
    sign-in inside an unattended suite, and the failure would be a run that
    hangs waiting for a browser nobody is watching.
    """

    from pathlib import Path

    conftest = Path(__file__).resolve().parent / "fabric" / "conftest.py"
    source = conftest.read_text(encoding="utf-8")

    assert "prefer_cli_credential" in source
    assert "desktop_credential" not in source


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

    def build(*, cached: bool):
        made.append(cached)
        return with_a_cache if cached else without_one

    monkeypatch.setattr(auth, "_interactive_browser", build)
    monkeypatch.setattr(auth, "_warned", False)
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

    def build(*, cached: bool):
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

    def build(*, cached: bool):
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

    def build(*, cached: bool):
        made.append(cached)
        return refusing if cached else working

    monkeypatch.setattr(auth, "_interactive_browser", build)
    monkeypatch.setattr(auth, "_warned", False)

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

    def build(*, cached: bool):
        made.append(cached)
        return refusing if cached else working

    monkeypatch.setattr(auth, "_interactive_browser", build)
    monkeypatch.setattr(auth, "_warned", False)

    auth.BrowserSignIn().get_token(auth.FABRIC_SCOPE)

    assert made == [True, False]


@weaver_test()
def test_an_unrelated_failure_propagates_and_opens_no_second_browser(
    monkeypatch, capsys
):
    """The inverse claim: a defect elsewhere is not read as a missing keyring."""

    made: list[bool] = []
    broken = _NoSecureStorage(raising=RuntimeError("something else went wrong"))

    def build(*, cached: bool):
        made.append(cached)
        return broken

    monkeypatch.setattr(auth, "_interactive_browser", build)
    monkeypatch.setattr(auth, "_warned", False)

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
        auth, "_interactive_browser", lambda *, cached: made.append(cached) or broken
    )
    monkeypatch.setattr(auth, "_warned", False)

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
        auth, "_interactive_browser", lambda *, cached: made.append(cached) or broken
    )
    monkeypatch.setattr(auth, "_warned", False)

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
            lambda *, cached: made.append(cached) or (refusing if cached else working),
        )
        monkeypatch.setattr(auth, "_warned", False)

        assert auth.BrowserSignIn().get_token(auth.FABRIC_SCOPE).token == "a-token"
        assert made == [True, False]

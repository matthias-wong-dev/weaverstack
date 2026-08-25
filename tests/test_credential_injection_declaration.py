"""Which credential Weaver authenticates with, and who decides.

Choosing a credential is a caller's policy. Core accepts an injected one and
otherwise uses the library default without pinning a chain, so importing Weaver
imposes no credential choice on the process that imported it.

Two properties matter beyond "it is used":

.. code-block:: text

    checked where it is supplied      not where it is first used
    acquired when it is needed        not when the Session is opened

The first because a Session acquires lazily, so a wrong object handed to
``weaver.session()`` would otherwise surface during whichever operation first
reached Fabric, a long way from the line that caused it. The second because a
Session that acquired a token on open would make holding one expensive.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

import weaver
from weaver.errors import ConfigError
from weaver.fabric.auth import checked_credential


class _Credential:
    """The shape ``azure.core`` names, and a record of what was asked of it."""

    def __init__(self):
        self.scopes: list[tuple] = []

    def get_token(self, *scopes, **_):
        self.scopes.append(scopes)

        class _Token:
            token = "a-token"
            expires_on = 4102444800

        return _Token()


# --- what counts as a credential ----------------------------------------------


@weaver_test()
def test_anything_offering_get_token_is_accepted():
    """Structural, not an isinstance check.

    A caller may pass a wrapper, a fake, or a credential from a library Weaver
    does not import. What every one of them has is ``get_token``.
    """

    supplied = _Credential()

    assert checked_credential(supplied) is supplied


@weaver_test()
def test_none_stays_none_and_means_the_library_default():
    assert checked_credential(None) is None


@weaver_test()
def test_an_object_without_get_token_is_refused_by_name():
    with pytest.raises(ConfigError, match="get_token"):
        checked_credential(object())


@weaver_test()
def test_a_get_token_that_is_not_callable_is_refused():
    class _Wrong:
        get_token = "not a method"

    with pytest.raises(ConfigError, match="get_token"):
        checked_credential(_Wrong())


# --- where it is checked, and when it is used ---------------------------------


@weaver_test()
def test_a_wrong_credential_fails_at_the_call_that_supplied_it():
    """The point of checking early: the traceback names the caller's line."""

    with pytest.raises(ConfigError, match="get_token"):
        weaver.session(
            workspace="Demo", catalogue="Warehouse/Weaver", credential=object()
        )


@weaver_test()
def test_opening_a_session_with_a_credential_acquires_no_token():
    """Lazy, so holding a Session stays cheap.

    The credential records every call, and there should be none: a Session that
    acquired on open would pay a network round trip for a caller who had not yet
    asked for anything.
    """

    supplied = _Credential()

    with weaver.session(
        workspace="Demo", catalogue="Warehouse/Weaver", credential=supplied
    ):
        pass

    assert supplied.scopes == []


@weaver_test()
def test_the_supplied_credential_is_what_the_session_authenticates_with():
    """And it reaches the token provider rather than being kept and ignored."""

    supplied = _Credential()

    with weaver.session(
        workspace="Demo", catalogue="Warehouse/Weaver", credential=supplied
    ) as session:
        provider = session.scope().token_provider()
        assert provider() == "a-token"

    assert supplied.scopes, "the injected credential was never asked for a token"


@weaver_test()
def test_without_one_nothing_is_pinned():
    """Core imposes no chain. The default is chosen when it is needed."""

    with weaver.session(workspace="Demo", catalogue="Warehouse/Weaver") as session:
        assert session.scope()._credential is None

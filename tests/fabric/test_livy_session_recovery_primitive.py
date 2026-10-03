"""What Fabric answers for a refused statement and for a session it has ended."""

from __future__ import annotations

import time

import pytest
from support.weaver_test import weaver_test


@pytest.mark.slow
@weaver_test(remote=True)
def test_a_refused_statement_keeps_the_session_and_an_ended_one_is_replaced(
    livy_session,
):
    from weaver.fabric.livy import LivyRefused, _call

    first = livy_session.session_url
    # Fabric refuses a statement body over 4 MiB before accepting it.
    with pytest.raises(LivyRefused, match="4194304"):
        livy_session.run("x = 0\n" * 800_000)
    assert livy_session.run("emit(1)").payload == 1
    assert livy_session.session_url == first

    restarts = []
    announce, livy_session.restarted = (
        livy_session.restarted,
        lambda: restarts.append(livy_session.session_url),
    )
    _call("DELETE", first, livy_session.token, expected=(200, 202, 204, 404))
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        state = _call("GET", first, livy_session.token, expected=(200, 404))
        if (state.get("state") or "dead").lower() in {"dead", "killed", "error"}:
            break
        time.sleep(3)

    try:
        observed = livy_session.run("import weaver\nemit(weaver.__version__)").payload
    finally:
        livy_session.restarted = announce

    assert observed
    assert restarts == [livy_session.session_url]
    assert livy_session.session_url != first

"""A Notebook can name the expected workspace without retargeting a bundle."""

from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test


@weaver_test()
def test_notebook_install_refuses_a_different_sealed_workspace_before_a_session(
    monkeypatch,
):
    import weaver.build_bundle
    import weaver.build_bundle.execution
    import weaver.sessions.host
    from weaver.workspaces import Workspace

    loaded = SimpleNamespace(plan=SimpleNamespace(execution=object()))
    monkeypatch.setattr(
        weaver.build_bundle, "load_bundle", lambda *args, **kwargs: loaded
    )
    monkeypatch.setattr(
        weaver.build_bundle.execution,
        "execution_workspace",
        lambda *args: Workspace(workspace="Parcel Production"),
    )

    def forbidden(*args, **kwargs):
        raise AssertionError("a workspace mismatch must not acquire a Session")

    monkeypatch.setattr(weaver.sessions.host, "use_or_create_session", forbidden)
    with pytest.raises(weaver.CommandError, match="Parcel Production"):
        weaver.install("./bundle", workspace="Parcel Development")

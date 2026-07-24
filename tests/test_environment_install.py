"""Unit tests for the Environment install helpers that need no Fabric.

The Fabric round-trip is exercised by the opt-in integration path; here we pin
the pure logic: which files count as Weaver wheels, how a version is read back
from a wheel name, and — the safety-critical one — that stale-wheel cleanup only
ever removes ``weaverstack`` wheels.
"""

from __future__ import annotations

import types
from pathlib import Path

import pytest

from weaver.fabric import environment as env_mod
from weaver.fabric.environment import (
    _version_from_wheel,
    delete_stale_wheels,
    find_or_create_environment,
    is_weaver_wheel,
    read_published,
    staged_wheels,
)
from weaver.errors import CommandError
from weaver.fabric.client import FabricClient, FabricError
from weaver.fabric.resources import Item, ItemNotFoundError, Workspace


def test_only_weaver_wheels_are_recognised():
    assert is_weaver_wheel("weaverstack-0.1.0-py3-none-any.whl")
    assert is_weaver_wheel("weaverstack-0.1.1.dev0+g7148d2d4a.d20260723-py3-none-any.whl")
    assert not is_weaver_wheel("pandas-2.2.0-py3-none-any.whl")
    assert not is_weaver_wheel("weaverstack-0.1.0.tar.gz")


def test_version_is_read_back_from_the_wheel_name():
    assert _version_from_wheel("weaverstack-0.1.0-py3-none-any.whl") == "0.1.0"
    assert (
        _version_from_wheel("weaverstack-0.1.1.dev20260723230242-py3-none-any.whl")
        == "0.1.1.dev20260723230242"
    )


def test_staged_wheels_reads_the_custom_library_list():
    staging = {"customLibraries": {"wheelFiles": ["weaverstack-0.1.0-py3-none-any.whl"]}}
    assert staged_wheels(staging) == ["weaverstack-0.1.0-py3-none-any.whl"]
    assert staged_wheels({}) == []


class _RecordingClient:
    """A client that records DELETE paths and nothing else."""

    def __init__(self):
        self.deleted: list[str] = []

    def request(self, method, path, *, expected=()):
        assert method == "DELETE"
        self.deleted.append(path)


def _env() -> Item:
    return Item(id="env1", name="Weaver", type="Environment", workspace_id="ws1")


def test_stale_weaver_wheels_are_removed_but_the_kept_one_is_not():
    client = _RecordingClient()
    staged = [
        "weaverstack-0.1.0-py3-none-any.whl",  # stale
        "weaverstack-0.0.9-py3-none-any.whl",  # stale
        "weaverstack-0.2.0-py3-none-any.whl",  # the one we keep
    ]
    removed = delete_stale_wheels(_env(), "weaverstack-0.2.0-py3-none-any.whl", staged, client=client)
    assert set(removed) == {"weaverstack-0.1.0-py3-none-any.whl", "weaverstack-0.0.9-py3-none-any.whl"}
    assert all("weaverstack-0.2.0" not in path for path in client.deleted)


def test_unrelated_custom_libraries_are_never_deleted():
    client = _RecordingClient()
    staged = ["pandas-2.2.0-py3-none-any.whl", "some_internal_lib-1.0-py3-none-any.whl"]
    removed = delete_stale_wheels(_env(), "weaverstack-0.2.0-py3-none-any.whl", staged, client=client)
    assert removed == []
    assert client.deleted == []


class _NeverCreateClient:
    def request(self, *args, **kwargs):
        pytest.fail("must not create an Environment after an unexpected lookup failure")


def test_environment_creation_only_follows_item_not_found(monkeypatch):
    def fail_lookup(*args, **kwargs):
        raise CommandError("duplicate Environment matches")

    monkeypatch.setattr(env_mod, "find_item", fail_lookup)

    with pytest.raises(CommandError, match="duplicate"):
        find_or_create_environment(
            Workspace("ws1", "WS"), "weaver", client=_NeverCreateClient()
        )


class _CreatedResponse:
    status_code = 201

    def json(self):
        return {"id": "env1"}


class _CreateClient:
    def __init__(self):
        self.created = False

    def request(self, method, path, *, payload, expected):
        self.created = True
        return _CreatedResponse()


def test_environment_creation_follows_item_not_found(monkeypatch):
    def missing(*args, **kwargs):
        raise ItemNotFoundError("missing")

    monkeypatch.setattr(env_mod, "find_item", missing)
    client = _CreateClient()

    item, created = find_or_create_environment(
        Workspace("ws1", "WS"), "weaver", client=client
    )

    assert (item.id, created, client.created) == ("env1", True, True)


class _PublishedClient:
    def __init__(self, status_code):
        self.status_code = status_code

    def get_json(self, path):
        raise FabricError("published-library lookup failed", status_code=self.status_code)


def test_never_published_environment_is_empty():
    assert read_published(_env(), client=_PublishedClient(404)) == {}


@pytest.mark.parametrize("status_code", [401, 429, 500, None])
def test_published_library_failures_other_than_404_are_re_raised(status_code):
    with pytest.raises(FabricError, match="lookup failed"):
        read_published(_env(), client=_PublishedClient(status_code))


def test_fabric_client_preserves_failure_status(monkeypatch):
    response = types.SimpleNamespace(status_code=429, text="slow down", content=b"")
    monkeypatch.setattr("requests.request", lambda *args, **kwargs: response)

    with pytest.raises(FabricError) as info:
        FabricClient(token="token").get_json("workspaces")

    assert info.value.status_code == 429


# --- install() diff-and-skip decisions, without touching Fabric --------------


def _wire(monkeypatch, *, published: dict, staged: dict, state: str, wheel_name: str):
    """Stub every Fabric call install() makes; record uploads and publishes."""

    events: dict[str, object] = {"uploaded_yml": False, "uploaded_wheel": False, "published": False}

    monkeypatch.setattr(env_mod, "build_wheel", lambda root=None, **k: Path(f"dist/{wheel_name}"))
    monkeypatch.setattr(
        env_mod, "find_workspace", lambda name, client=None: env_mod.Workspace("ws1", name)
    )
    monkeypatch.setattr(
        env_mod,
        "find_or_create_environment",
        lambda ws, name, *, client: (env_mod.Item("env1", name, "Environment", ws.id), False),
    )
    monkeypatch.setattr(env_mod, "read_published", lambda env, *, client: published)
    monkeypatch.setattr(env_mod, "read_staging", lambda env, *, client: staged)
    monkeypatch.setattr(env_mod, "publish_state", lambda env, *, client: state)

    def _yml(env, definition, *, client):
        events["uploaded_yml"] = True

    def _wheel(env, wheel, *, client):
        events["uploaded_wheel"] = True

    def _pub(env, *, client, **k):
        events["published"] = True
        return "Success"

    monkeypatch.setattr(env_mod, "upload_environment_yml", _yml)
    monkeypatch.setattr(env_mod, "upload_wheel", _wheel)
    monkeypatch.setattr(env_mod, "delete_stale_wheels", lambda *a, **k: [])
    monkeypatch.setattr(env_mod, "publish_and_wait", _pub)
    return events


WANTED_YML = None  # resolved from the real definition at call time


def _published_body(wheel: str, yml: str) -> dict:
    return {"customLibraries": {"wheelFiles": [wheel]}, "environmentYml": yml}


def test_unchanged_source_skips_publish(monkeypatch):
    yml = env_mod.project_root().joinpath(env_mod.ENVIRONMENT_DEFINITION).read_text()
    wheel = "weaverstack-0.1.1.dev999-py3-none-any.whl"
    events = _wire(
        monkeypatch,
        published=_published_body(wheel, yml),
        staged=_published_body(wheel, yml),
        state="Success",
        wheel_name=wheel,
    )
    result = env_mod.install("WS", "weaver", client=object())
    assert result.wheel_changed is False
    assert result.dependencies_changed is False
    assert result.publish_status == "AlreadyInstalled"
    assert events == {"uploaded_yml": False, "uploaded_wheel": False, "published": False}


def test_code_change_uploads_only_the_wheel_and_publishes(monkeypatch):
    yml = env_mod.project_root().joinpath(env_mod.ENVIRONMENT_DEFINITION).read_text()
    old_wheel = "weaverstack-0.1.1.dev111-py3-none-any.whl"
    new_wheel = "weaverstack-0.1.1.dev222-py3-none-any.whl"
    events = _wire(
        monkeypatch,
        published=_published_body(old_wheel, yml),  # deps already match
        staged=_published_body(old_wheel, yml),
        state="Success",
        wheel_name=new_wheel,
    )
    result = env_mod.install("WS", "weaver", client=object())
    assert result.wheel_changed is True
    assert result.dependencies_changed is False
    assert events["uploaded_wheel"] is True
    assert events["uploaded_yml"] is False  # deps untouched
    assert events["published"] is True


def test_no_publish_flag_never_publishes(monkeypatch):
    yml = env_mod.project_root().joinpath(env_mod.ENVIRONMENT_DEFINITION).read_text()
    events = _wire(
        monkeypatch,
        published=_published_body("weaverstack-0.1.1.dev111-py3-none-any.whl", yml),
        staged=_published_body("weaverstack-0.1.1.dev111-py3-none-any.whl", yml),
        state="Success",
        wheel_name="weaverstack-0.1.1.dev222-py3-none-any.whl",
    )
    result = env_mod.install("WS", "weaver", client=object(), publish=False)
    assert result.publish_status == "Skipped"
    assert events["published"] is False

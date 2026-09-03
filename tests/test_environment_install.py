"""Publishing Weaver into a Fabric Environment, in both modes and both sources.

``--path`` and ``--dev`` are independent, so the four combinations are the
subject here: where the definition comes from, and how Weaver is supplied.
"""

from __future__ import annotations

import base64
from pathlib import Path

import pytest
from support.weaver_test import weaver_test

from weaver.errors import CommandError
from weaver.fabric import environment as env_mod
from weaver.fabric.client import FabricError
from weaver.fabric.environment import (
    is_weaver_wheel,
    library_wheels,
    resolve_environment_owner,
)
from weaver.fabric.environment_definition import (
    CUSTOM_LIBRARIES,
    EXTERNAL_LIBRARIES,
    PLATFORM,
    SPARK_COMPUTE,
)
from weaver.fabric.resources import Item, ItemNotFoundError, WorkspaceItem

WHEEL = "weaverstack-0.1.2.dev99-py3-none-any.whl"
OTHER_WHEEL = "userpackage-1.0-py3-none-any.whl"
PLATFORM_JSON = b'{"metadata": {"type": "Environment", "displayName": "Runtime"}}'
SPARK_YML = b"runtime_version: 1.3\n"


def _env() -> Item:
    return Item(id="env-id", name="Runtime", type="Environment", workspace_id="ws-id")


def _workspace() -> WorkspaceItem:
    return WorkspaceItem(id="ws-id", name="Analytics")


def _libraries(*entries) -> dict:
    return {"libraries": list(entries)}


def _custom(filename: str) -> dict:
    return {"name": filename, "libraryType": "Custom"}


def _external(name: str, version: str = "") -> dict:
    return {"name": name, "libraryType": "External", "version": version}


def _built_wheel(tmp_path: Path, name: str = WHEEL) -> Path:
    path = tmp_path / name
    path.write_bytes(b"wheel bytes")
    return path


# --- what Weaver owns ----------------------------------------------------------


@weaver_test()
def test_only_weaver_wheels_are_owned():
    assert is_weaver_wheel(WHEEL)
    assert not is_weaver_wheel(OTHER_WHEEL)
    assert not is_weaver_wheel("weaverstack-0.1.0.tar.gz")


@weaver_test()
def test_ga_custom_libraries_are_read_by_name():
    libraries = _libraries(_custom(WHEEL), _external("pyyaml"), _custom(OTHER_WHEEL))

    assert library_wheels(libraries) == [WHEEL, OTHER_WHEEL]


@weaver_test()
def test_environment_reference_owner_resolution():
    assert resolve_environment_owner("Analytics", "Runtime")[0] == "Analytics"
    assert resolve_environment_owner(None, "Platform/Runtime")[0] == "Platform"


@weaver_test()
def test_qualified_environment_conflicting_with_workspace_is_rejected():
    with pytest.raises(CommandError, match="conflicts with workspace"):
        resolve_environment_owner("Analytics", "Platform/Runtime")


@weaver_test()
def test_an_unqualified_environment_needs_a_workspace():
    with pytest.raises(CommandError, match="requires --workspace"):
        resolve_environment_owner(None, "Runtime")


# --- the library route: the Environment is authoritative ------------------------


class _LibraryClient:
    """A Fabric client recording what a library-route publication asked for."""

    def __init__(self, *, staged=(), external="", state="Success"):
        self.staged = list(staged)
        self.external = external
        self.state = state
        self.imported: list[str] = []
        self.uploaded: list[str] = []
        self.deleted: list[str] = []
        self.published = 0
        self.api_base_url = "https://api.invalid/v1"
        self.token = "token"
        self.timeout = 30

    def paged(self, path, *, key, not_found_empty=False):
        return list(self.staged)

    def get_json(self, path):
        if path.endswith("/sparkcompute?beta=false"):
            return {"runtimeVersion": "1.3"}
        return {"properties": {"publishDetails": {"state": self.state}}}

    def request(self, method, path, *, payload=None, expected=()):
        if method == "GET" and path.endswith("exportExternalLibraries"):
            return _Response(200, content=self.external.encode("utf-8"))
        if method == "DELETE":
            self.deleted.append(path.rsplit("/", 1)[-1])
            return _Response(200)
        if path.endswith("/publish?beta=false"):
            self.published += 1
            return _Response(202)
        raise AssertionError(f"unexpected {method} {path}")


class _Response:
    def __init__(self, status_code, content=b"", payload=None, headers=None):
        self.status_code = status_code
        self.content = content
        self.text = content.decode("utf-8", "replace")
        self._payload = payload
        self.headers = headers or {}

    def json(self):
        return self._payload


def _library_publication(monkeypatch, client, *, dev=False, wheel=None):
    """Run one library-route publication against a recording client."""

    monkeypatch.setattr(
        env_mod, "find_workspace", lambda name, client=None: _workspace()
    )
    monkeypatch.setattr(
        env_mod,
        "find_item",
        lambda workspace, name, item_type=None, client=None: _env(),
    )
    monkeypatch.setattr(
        env_mod,
        "import_external_libraries",
        lambda item, text, client: client.imported.append(text),
    )
    monkeypatch.setattr(
        env_mod,
        "upload_wheel",
        lambda item, path, client: client.uploaded.append(path.name),
    )
    if dev:
        monkeypatch.setattr(env_mod, "build_wheel", lambda root: wheel)
        monkeypatch.setattr(
            env_mod, "runtime_requirements", lambda root: ("pyyaml", "mssql-python")
        )
    return env_mod.publish_environment(
        "Analytics", "Runtime", dev=dev, client=client, root=Path(".")
    )


@weaver_test()
def test_released_adds_one_weaver_requirement_and_keeps_the_rest(monkeypatch):
    """Nothing but Weaver's own libraries changes, and the rest is untouched."""

    client = _LibraryClient(
        staged=[_custom(OTHER_WHEEL), _external("fuzzywuzzy", "0.18.0")],
        external="dependencies:\n  - pip:\n      - fuzzywuzzy==0.18.0\n",
    )

    result = _library_publication(monkeypatch, client)

    assert "weaverstack" in client.imported[0]
    assert "fuzzywuzzy==0.18.0" in client.imported[0]
    assert client.uploaded == []
    assert client.deleted == []
    assert result.mode == "released"
    assert result.weaver_requirement == "weaverstack"
    assert result.published


@weaver_test()
def test_released_removes_a_weaver_custom_wheel(monkeypatch):
    """Switching back from --dev must not leave the wheel taking precedence."""

    client = _LibraryClient(staged=[_custom(WHEEL), _custom(OTHER_WHEEL)])

    result = _library_publication(monkeypatch, client)

    assert client.deleted == [WHEEL]
    assert OTHER_WHEEL not in client.deleted
    assert result.removed_wheels == (WHEEL,)


@weaver_test()
def test_development_uploads_the_checkout_wheel_and_names_its_requirements(
    monkeypatch, tmp_path
):
    """A Fabric custom wheel installs no dependencies, so Weaver's are listed."""

    client = _LibraryClient(
        external="dependencies:\n  - pip:\n      - weaverstack==0.4.0\n"
    )
    wheel = _built_wheel(tmp_path)

    result = _library_publication(monkeypatch, client, dev=True, wheel=wheel)

    imported = client.imported[0]
    assert "weaverstack==0.4.0" not in imported
    assert "pyyaml" in imported and "mssql-python" in imported
    assert client.uploaded == [WHEEL]
    assert result.mode == "dev"
    assert result.wheel_filename == WHEEL
    assert result.weaver_requirement is None


@weaver_test()
def test_development_replaces_a_stale_weaver_wheel(monkeypatch, tmp_path):
    stale = "weaverstack-0.1.2.dev1-py3-none-any.whl"
    client = _LibraryClient(staged=[_custom(stale), _custom(OTHER_WHEEL)])
    wheel = _built_wheel(tmp_path)

    result = _library_publication(monkeypatch, client, dev=True, wheel=wheel)

    assert client.uploaded == [WHEEL]
    assert client.deleted == [stale]
    assert result.removed_wheels == (stale,)


@weaver_test()
def test_an_environment_already_carrying_weaver_is_a_noop(monkeypatch, tmp_path):
    """The publish is minutes, so an unchanged Environment does not pay for one."""

    wheel = _built_wheel(tmp_path)
    client = _LibraryClient(
        staged=[_custom(WHEEL)],
        external="dependencies:\n  - pip:\n      - pyyaml\n      - mssql-python\n",
    )

    result = _library_publication(monkeypatch, client, dev=True, wheel=wheel)

    assert client.imported == []
    assert client.uploaded == []
    assert client.published == 0
    assert result.action == "unchanged"
    assert result.published is False
    assert result.publish_status == "AlreadyInstalled"


@weaver_test()
def test_a_missing_environment_says_how_to_get_one(monkeypatch):
    def missing(workspace, name, item_type=None, client=None):
        raise ItemNotFoundError(name)

    monkeypatch.setattr(
        env_mod, "find_workspace", lambda name, client=None: _workspace()
    )
    monkeypatch.setattr(env_mod, "find_item", missing)

    with pytest.raises(CommandError, match="--path, which creates it"):
        env_mod.publish_environment("Analytics", "Runtime", client=_LibraryClient())


# --- the definition route: the local directory is authoritative -----------------


class _DefinitionClient:
    """A Fabric client recording what a definition-route publication sent."""

    def __init__(self, *, current=None, state="Success", missing=False):
        self.current = current or {}
        self.state = state
        self.missing = missing
        self.sent: list[dict] = []
        self.created: list[dict] = []
        self.published = 0
        self.api_base_url = "https://api.invalid/v1"
        self.token = "token"
        self.timeout = 30

    def get_json(self, path):
        return {"properties": {"publishDetails": {"state": self.state}}}

    def request(self, method, path, *, payload=None, expected=()):
        if path.endswith("/getDefinition"):
            return _Response(
                200,
                payload={
                    "definition": {
                        "parts": [
                            {
                                "path": name,
                                "payload": base64.b64encode(content).decode("ascii"),
                                "payloadType": "InlineBase64",
                            }
                            for name, content in self.current.items()
                        ]
                    }
                },
            )
        if "updateDefinition" in path:
            self.sent.append(payload)
            return _Response(200)
        if path.endswith("/environments"):
            self.created.append(payload)
            return _Response(201)
        if path.endswith("/publish?beta=false"):
            self.published += 1
            return _Response(202)
        raise AssertionError(f"unexpected {method} {path}")

    def wait_for_operation(self, response, **kwargs):
        return {}


def _local(tmp_path: Path, **parts) -> Path:
    root = tmp_path / "Runtime.Environment"
    root.mkdir(parents=True, exist_ok=True)
    for relative, content in parts.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    return root


def _sent_parts(client) -> dict:
    payload = client.sent[-1] if client.sent else client.created[-1]
    return {
        part["path"]: base64.b64decode(part["payload"])
        for part in payload["definition"]["parts"]
    }


def _definition_publication(monkeypatch, client, path, *, dev=False, wheel=None):
    monkeypatch.setattr(
        env_mod, "find_workspace", lambda name, client=None: _workspace()
    )
    if client.missing:

        def missing(workspace, name, item_type=None, client=None):
            if client is None or not getattr(missing, "created", False):
                missing.created = True
                raise ItemNotFoundError(name)
            return _env()

        monkeypatch.setattr(env_mod, "find_item", missing)
    else:
        monkeypatch.setattr(
            env_mod,
            "find_item",
            lambda workspace, name, item_type=None, client=None: _env(),
        )
    if dev:
        monkeypatch.setattr(env_mod, "build_wheel", lambda root: wheel)
        monkeypatch.setattr(env_mod, "runtime_requirements", lambda root: ("pyyaml",))
    return env_mod.publish_environment(
        "Analytics", path=path, dev=dev, client=client, root=Path(".")
    )


@weaver_test()
def test_the_local_definition_is_sent_whole_with_weaver_overlaid(monkeypatch, tmp_path):
    """Everything the user authored survives; Weaver adds only its own."""

    path = _local(
        tmp_path,
        **{
            PLATFORM: PLATFORM_JSON,
            SPARK_COMPUTE: SPARK_YML,
            f"{CUSTOM_LIBRARIES}{OTHER_WHEEL}": b"user bytes",
        },
    )
    client = _DefinitionClient(current={PLATFORM: b"stale"})

    result = _definition_publication(monkeypatch, client, path)

    parts = _sent_parts(client)
    assert parts[PLATFORM] == PLATFORM_JSON
    assert parts[SPARK_COMPUTE] == SPARK_YML
    assert parts[f"{CUSTOM_LIBRARIES}{OTHER_WHEEL}"] == b"user bytes"
    assert b"weaverstack" in parts[EXTERNAL_LIBRARIES]
    assert result.action == "updated"
    assert result.source_path == str(path)


@weaver_test()
def test_the_local_directory_is_never_written_to(monkeypatch, tmp_path):
    """A generated wheel must not dirty the checkout it was built from."""

    path = _local(tmp_path, **{PLATFORM: PLATFORM_JSON})
    before = {
        entry.relative_to(path).as_posix(): entry.read_bytes()
        for entry in path.rglob("*")
        if entry.is_file()
    }
    wheel = _built_wheel(tmp_path)

    _definition_publication(
        monkeypatch, _DefinitionClient(), path, dev=True, wheel=wheel
    )

    after = {
        entry.relative_to(path).as_posix(): entry.read_bytes()
        for entry in path.rglob("*")
        if entry.is_file()
    }
    assert after == before


@weaver_test()
def test_development_overlays_the_wheel_and_drops_the_pypi_requirement(
    monkeypatch, tmp_path
):
    path = _local(
        tmp_path,
        **{
            EXTERNAL_LIBRARIES: b"dependencies:\n  - pip:\n      - weaverstack==0.4.0\n",
            f"{CUSTOM_LIBRARIES}weaverstack-0.0.1-py3-none-any.whl": b"stale",
        },
    )
    wheel = _built_wheel(tmp_path)
    client = _DefinitionClient()

    result = _definition_publication(monkeypatch, client, path, dev=True, wheel=wheel)

    parts = _sent_parts(client)
    assert f"{CUSTOM_LIBRARIES}{WHEEL}" in parts
    assert "weaverstack-0.0.1-py3-none-any.whl" not in str(parts)
    assert b"weaverstack==0.4.0" not in parts[EXTERNAL_LIBRARIES]
    assert b"pyyaml" in parts[EXTERNAL_LIBRARIES]
    assert result.wheel_filename == WHEEL
    assert result.weaver_requirement is None


@weaver_test()
def test_a_missing_environment_is_created_from_the_definition(monkeypatch, tmp_path):
    path = _local(tmp_path, **{PLATFORM: PLATFORM_JSON})
    client = _DefinitionClient(missing=True)

    result = _definition_publication(monkeypatch, client, path)

    assert client.created, "a missing Environment is created with its definition"
    assert client.created[-1]["displayName"] == "Runtime"
    assert result.action == "created"
    assert result.published


@weaver_test()
def test_an_identical_definition_does_not_republish(monkeypatch, tmp_path):
    path = _local(tmp_path, **{PLATFORM: PLATFORM_JSON})
    overlaid = b"dependencies:\n  - pip:\n      - weaverstack\n"
    client = _DefinitionClient(
        current={PLATFORM: PLATFORM_JSON, EXTERNAL_LIBRARIES: overlaid}
    )

    result = _definition_publication(monkeypatch, client, path)

    assert client.sent == []
    assert client.published == 0
    assert result.action == "unchanged"
    assert result.published is False


@weaver_test()
def test_a_rebuilt_wheel_with_the_same_version_is_not_a_change(monkeypatch, tmp_path):
    """The version is content addressed; the zip around it is not reproducible.

    Comparing the wheel's bytes would republish on every run of an unchanged
    checkout, which costs minutes and changes nothing.
    """

    path = _local(tmp_path)
    wheel = _built_wheel(tmp_path)
    client = _DefinitionClient(
        current={
            EXTERNAL_LIBRARIES: b"dependencies:\n  - pip:\n      - pyyaml\n",
            f"{CUSTOM_LIBRARIES}{WHEEL}": b"different compression, same version",
        }
    )

    result = _definition_publication(monkeypatch, client, path, dev=True, wheel=wheel)

    assert client.sent == []
    assert result.action == "unchanged"


@weaver_test()
def test_a_failed_publish_is_an_error(monkeypatch, tmp_path):
    path = _local(tmp_path, **{PLATFORM: PLATFORM_JSON})
    client = _DefinitionClient(state="Failed")

    with pytest.raises(FabricError, match="finished with status"):
        _definition_publication(monkeypatch, client, path)


@weaver_test()
def test_publishing_without_a_name_or_a_path_says_so():
    with pytest.raises(CommandError, match="or pass --path"):
        env_mod.publish_environment("Analytics", None, client=_LibraryClient())

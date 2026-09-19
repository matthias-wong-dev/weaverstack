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
    publishes_weaver,
    resolve_environment_owner,
)
from weaver.fabric.environment_definition import (
    CUSTOM_LIBRARIES,
    DISTRIBUTION,
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

    def __init__(
        self, *, staged=(), installed=None, external="", state="Success", states=()
    ):
        self.staged = list(staged)
        #: What Fabric has published. Defaults to the staged set, which is what
        #: an Environment whose last publish settled actually holds.
        self.installed = list(staged) if installed is None else list(installed)
        self.external = external
        self.state = state
        self.states = list(states)
        self.imported: list[str] = []
        self.uploaded: list[str] = []
        self.deleted: list[str] = []
        self.published = 0
        self.api_base_url = "https://api.invalid/v1"
        self.token = "token"
        self.timeout = 30

    def paged(self, path, *, key, not_found_empty=False):
        return list(self.staged if "/staging/" in path else self.installed)

    def get_json(self, path):
        if path.endswith("/sparkcompute?beta=false"):
            return {"runtimeVersion": "1.3"}
        state = self.states.pop(0) if self.states else self.state
        return {"properties": {"publishDetails": {"state": state}}}

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


# --- configured is not published ------------------------------------------------
#
# Staging is where a request is written and publishing is what installs it. The
# no-op therefore reads the published libraries: `AlreadyInstalled` means a
# session starting now imports Weaver.

WEAVER_PIP = "dependencies:\n  - pip:\n      - weaverstack\n"
DEV_PIP = "dependencies:\n  - pip:\n      - pyyaml\n      - mssql-python\n"
STALE_WHEEL = "weaverstack-0.1.2.dev1-py3-none-any.whl"


@weaver_test()
def test_a_requirement_staged_but_never_published_is_published(monkeypatch):
    """The reported failure: a matching definition over an unpublished runtime."""

    client = _LibraryClient(external=WEAVER_PIP, installed=[], states=[""])

    result = _library_publication(monkeypatch, client)

    assert client.published == 1
    assert result.action == "updated"
    assert result.published
    assert result.publish_status == "Success"


@weaver_test()
def test_a_published_weaver_requirement_is_a_verified_noop(monkeypatch):
    client = _LibraryClient(
        external=WEAVER_PIP, installed=[_external(DISTRIBUTION, "0.9.0")]
    )

    result = _library_publication(monkeypatch, client)

    assert client.published == 0
    assert result.action == "unchanged"
    assert result.publish_status == "AlreadyInstalled"


@weaver_test()
def test_an_environment_with_no_published_libraries_is_published(monkeypatch, tmp_path):
    client = _LibraryClient(
        staged=[_custom(WHEEL)], installed=[], external=DEV_PIP, states=[""]
    )

    _library_publication(monkeypatch, client, dev=True, wheel=_built_wheel(tmp_path))

    assert client.published == 1


@weaver_test()
def test_a_published_wheel_other_than_the_staged_one_is_published(
    monkeypatch, tmp_path
):
    """Published state differs from what is staged, so the request is not met."""

    client = _LibraryClient(
        staged=[_custom(WHEEL)], installed=[_custom(STALE_WHEEL)], external=DEV_PIP
    )

    result = _library_publication(
        monkeypatch, client, dev=True, wheel=_built_wheel(tmp_path)
    )

    assert client.published == 1
    assert result.action == "updated"


@weaver_test()
def test_a_publication_under_way_settles_before_the_decision(monkeypatch):
    """A running publish is neither a success to report nor one to restart."""

    client = _LibraryClient(
        external=WEAVER_PIP,
        installed=[_external(DISTRIBUTION)],
        states=["Running", "Success"],
    )

    result = _library_publication(monkeypatch, client)

    assert client.published == 0
    assert result.publish_status == "AlreadyInstalled"


@weaver_test()
def test_a_publication_under_way_that_fails_is_not_already_installed(monkeypatch):
    client = _LibraryClient(
        external=WEAVER_PIP,
        installed=[_external(DISTRIBUTION)],
        states=["Running", "Failed"],
        state="Failed",
    )

    with pytest.raises(FabricError, match="finished with status"):
        _library_publication(monkeypatch, client)

    assert client.published == 1


@weaver_test()
def test_a_failed_publication_is_never_reported_as_already_installed(monkeypatch):
    client = _LibraryClient(
        external=WEAVER_PIP, installed=[_external(DISTRIBUTION)], state="Failed"
    )

    with pytest.raises(FabricError, match="finished with status 'Failed'"):
        _library_publication(monkeypatch, client)


@weaver_test()
def test_a_published_state_that_cannot_be_read_is_not_a_noop(monkeypatch):
    """An unresolved publication state surfaces; it does not read as success."""

    class _Unreadable(_LibraryClient):
        def paged(self, path, *, key, not_found_empty=False):
            if "/staging/" in path:
                return list(self.staged)
            raise FabricError("reading published libraries returned 500")

    client = _Unreadable(external=WEAVER_PIP)

    with pytest.raises(FabricError, match="published libraries"):
        _library_publication(monkeypatch, client)

    assert client.published == 0


# --- what published libraries say about the runtime -----------------------------


@weaver_test()
def test_a_published_version_outside_the_requirement_is_not_installed():
    published = _libraries(_external(DISTRIBUTION, "0.8.0"))

    assert not publishes_weaver(published, wheel=None, requirement="weaverstack==0.9.0")
    assert publishes_weaver(published, wheel=None, requirement="weaverstack==0.8.0")
    assert publishes_weaver(published, wheel=None, requirement="weaverstack")


@weaver_test()
def test_a_published_package_with_no_reported_version_counts_as_present():
    """Fabric does not always resolve a version; presence is then what it says."""

    published = _libraries(_external(DISTRIBUTION))

    assert publishes_weaver(published, wheel=None, requirement="weaverstack==0.9.0")


@weaver_test()
def test_another_projects_libraries_do_not_stand_in_for_weaver():
    published = _libraries(_external("fuzzywuzzy", "0.18.0"), _custom(OTHER_WHEEL))

    assert not publishes_weaver(published, wheel=None, requirement="weaverstack")
    assert not publishes_weaver(published, wheel=WHEEL, requirement=None)


@weaver_test()
def test_a_development_publication_is_read_from_the_custom_wheels():
    published = _libraries(_custom(WHEEL), _external(DISTRIBUTION))

    assert publishes_weaver(published, wheel=WHEEL, requirement=None)
    assert not publishes_weaver(published, wheel=STALE_WHEEL, requirement=None)


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

    def __init__(self, *, current=None, installed=None, state="Success", missing=False):
        self.current = current or {}
        #: What Fabric has published. The default settles the definition tests
        #: that are about the definition rather than about the publication.
        self.installed = (
            [_external(DISTRIBUTION), _custom(WHEEL)]
            if installed is None
            else list(installed)
        )
        self.state = state
        self.components: dict = {}
        self.missing = missing
        self.sent: list[dict] = []
        self.created: list[dict] = []
        self.published = 0
        self.api_base_url = "https://api.invalid/v1"
        self.token = "token"
        self.timeout = 30

    def paged(self, path, *, key, not_found_empty=False):
        return list(self.installed)

    def get_json(self, path):
        details = {"state": self.state}
        if self.components:
            details["componentPublishInfo"] = self.components
        return {"properties": {"publishDetails": details}}

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
def test_an_identical_definition_with_nothing_published_is_still_published(
    monkeypatch, tmp_path
):
    """The definition matches, and Fabric has published none of it."""

    path = _local(tmp_path, **{PLATFORM: PLATFORM_JSON})
    overlaid = b"dependencies:\n  - pip:\n      - weaverstack\n"
    client = _DefinitionClient(
        current={PLATFORM: PLATFORM_JSON, EXTERNAL_LIBRARIES: overlaid}, installed=[]
    )

    result = _definition_publication(monkeypatch, client, path)

    assert client.sent, "an unpublished definition was left staged"
    assert client.published == 1
    assert result.action == "updated"
    assert result.published


@weaver_test()
def test_a_definition_whose_wheel_is_not_the_published_one_is_published(
    monkeypatch, tmp_path
):
    path = _local(tmp_path)
    client = _DefinitionClient(
        current={
            EXTERNAL_LIBRARIES: b"dependencies:\n  - pip:\n      - pyyaml\n",
            f"{CUSTOM_LIBRARIES}{WHEEL}": b"same version, different bytes",
        },
        installed=[_custom(STALE_WHEEL)],
    )

    result = _definition_publication(
        monkeypatch, client, path, dev=True, wheel=_built_wheel(tmp_path)
    )

    assert client.published == 1
    assert result.action == "updated"


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


# --- the request shapes Fabric accepts -----------------------------------------


@weaver_test()
def test_the_external_library_import_sends_the_file_as_octet_stream(monkeypatch):
    """Fabric answers a multipart body with EnvironmentValidationFailed.

    Verified against the tenant: the import takes the file's bytes with
    ``application/octet-stream``, as the custom library upload does.
    """

    sent = {}

    def record(method, url, *, headers, data=None, timeout=None, **keywords):
        sent.update(method=method, url=url, headers=headers, data=data, extra=keywords)
        return _Response(200)

    monkeypatch.setattr(env_mod, "send", record)

    env_mod.import_external_libraries(
        _env(),
        "dependencies:\n  - pip:\n      - weaverstack\n",
        client=_LibraryClient(),
    )

    assert sent["method"] == "POST"
    assert sent["url"].endswith("/staging/libraries/importExternalLibraries")
    assert sent["headers"]["Content-Type"] == "application/octet-stream"
    assert sent["data"] == b"dependencies:\n  - pip:\n      - weaverstack\n"
    assert "files" not in sent["extra"]


@weaver_test()
def test_a_definition_fabric_reformatted_is_not_a_change(monkeypatch, tmp_path):
    """Fabric hands the text parts back the way it stores them.

    Measured against the tenant: a checkout written on Windows sends `\r\n` and
    reads back `\n`, and `runtime_version: '1.3'` reads back unquoted, which YAML
    then loads as a float. Comparing those bytes made every publication a change,
    and each one costs a Fabric publish.
    """

    path = _local(
        tmp_path,
        **{
            PLATFORM: b'{\r\n  "metadata": {\r\n    "type": "Environment"\r\n  }\r\n}',
            SPARK_COMPUTE: b"runtime_version: '1.3'\r\ndriver_cores: 4\r\n",
            EXTERNAL_LIBRARIES: b"dependencies:\n  - pip:\n      - weaverstack\n",
        },
    )
    client = _DefinitionClient(
        current={
            PLATFORM: b'{\n  "metadata": {\n    "type": "Environment"\n  }\n}',
            SPARK_COMPUTE: b"runtime_version: 1.3\ndriver_cores: 4\n",
            EXTERNAL_LIBRARIES: b"dependencies:\n  - pip:\n      - weaverstack\n",
        }
    )

    result = _definition_publication(monkeypatch, client, path)

    assert client.sent == []
    assert client.published == 0
    assert result.action == "unchanged"


@weaver_test()
def test_a_changed_setting_is_still_a_change(monkeypatch, tmp_path):
    """Reading the text parts as content must not read past a real edit."""

    path = _local(
        tmp_path,
        **{
            SPARK_COMPUTE: b"runtime_version: '1.3'\ndriver_cores: 8\n",
            EXTERNAL_LIBRARIES: b"dependencies:\n  - pip:\n      - weaverstack\n",
        },
    )
    client = _DefinitionClient(
        current={
            SPARK_COMPUTE: b"runtime_version: 1.3\ndriver_cores: 4\n",
            EXTERNAL_LIBRARIES: b"dependencies:\n  - pip:\n      - weaverstack\n",
        }
    )

    result = _definition_publication(monkeypatch, client, path)

    assert client.sent, "a changed driver_cores must reach Fabric"
    assert result.action == "updated"


@weaver_test()
def test_a_failed_publish_names_the_component(monkeypatch, tmp_path):
    """Fabric publishes settings and libraries apart and reports each."""

    path = _local(tmp_path, **{PLATFORM: PLATFORM_JSON})
    client = _DefinitionClient(state="Failed")
    client.components = {
        "sparkSettings": {"state": "Success"},
        "sparkLibraries": {"state": "Failed"},
    }

    with pytest.raises(FabricError, match="sparkLibraries did not settle"):
        _definition_publication(monkeypatch, client, path)


@pytest.mark.parametrize(
    "stored,local",
    [
        (b'{"value": 1}', b'{"value": "1"}'),
        (b'{"value": true}', b'{"value": "true"}'),
        (b'{"value": null}', b'{"value": "null"}'),
    ],
)
@weaver_test()
def test_a_platform_scalar_type_is_part_of_the_definition(
    monkeypatch, tmp_path, stored, local
):
    """Reading a text part as content must not erase what its scalars are.

    Normalisation covers what Fabric was seen to do, and retyping a JSON value
    is not among it.
    """

    path = _local(tmp_path, **{PLATFORM: local})
    client = _DefinitionClient(current={PLATFORM: stored})

    result = _definition_publication(monkeypatch, client, path)

    assert client.sent, "a retyped scalar is a different definition"
    assert result.action == "updated"


@weaver_test()
def test_a_spark_setting_retyped_is_still_a_change(monkeypatch, tmp_path):
    """Only ``runtime_version`` is compared as text, and only because Fabric
    returns it unquoted."""

    path = _local(
        tmp_path, **{SPARK_COMPUTE: b'enable_native_execution_engine: "false"\n'}
    )
    client = _DefinitionClient(
        current={SPARK_COMPUTE: b"enable_native_execution_engine: false\n"}
    )

    result = _definition_publication(monkeypatch, client, path)

    assert client.sent
    assert result.action == "updated"

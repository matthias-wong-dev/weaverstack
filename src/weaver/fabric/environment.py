"""Publish Weaver into a Fabric Environment.

Without ``--path``, only Weaver's staged libraries change. With ``--path``, the
local ``*.Environment`` definition is sent whole. Development mode installs the
checkout wheel and its Fabric dependencies instead of the PyPI requirement.
"""

from __future__ import annotations

import subprocess
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import quote

from ..errors import CommandError
from ..workspaces import EnvironmentRef
from .client import FabricClient, FabricError, send
from .environment_definition import (
    CUSTOM_LIBRARIES,
    DISTRIBUTION,
    EXTERNAL_LIBRARIES,
    PLATFORM,
    SPARK_COMPUTE,
    EnvironmentDefinition,
    definition_from_payload,
    definition_payload,
    development_external_libraries,
    environment_name_from_path,
    normalise_distribution,
    read_environment_definition,
    released_external_libraries,
    runtime_requirements,
    weaver_requirement,
)
from .environment_definition import (
    pip_entries as _pip_entries,
)
from .resources import (
    ENVIRONMENT,
    Item,
    ItemNotFoundError,
    WorkspaceItem,
    find_item,
    find_workspace,
)

WHEEL_PREFIX = f"{DISTRIBUTION}-"
WHEEL_SUFFIX = ".whl"

RELEASED = "released"
DEVELOPMENT = "dev"

CREATED = "created"
UPDATED = "updated"
UNCHANGED = "unchanged"


def project_root(module: Path | str | None = None) -> Path:
    """The Weaver source checkout this installation was imported from.

    ``module`` is the installed Weaver module path the search walks up from.

    A candidate ancestor is accepted only once its ``pyproject.toml`` names the
    Weaver distribution. A virtual environment nested under an unrelated project
    puts that project's ``pyproject.toml`` on the way up, and a ``--dev``
    publication that built it would publish the wrong package.
    """

    here = Path(module or __file__).resolve()
    for parent in here.parents:
        candidate = parent / "pyproject.toml"
        if candidate.is_file() and names_weaver(candidate):
            return parent
    raise CommandError(
        f"Cannot publish with --dev: no {DISTRIBUTION} source checkout was "
        f"found above {here}.\nInstall Weaver from the checkout in editable "
        "mode, or publish the released package without --dev."
    )


def names_weaver(pyproject: Path) -> bool:
    """Whether a ``pyproject.toml`` declares the Weaver distribution.

    Unreadable or malformed metadata is not a Weaver checkout. Failing the
    candidate keeps the search going instead of handing a build backend a
    project Weaver cannot identify.
    """

    import tomllib

    try:
        payload = tomllib.loads(pyproject.read_text("utf-8"))
    except (OSError, ValueError):
        return False
    declared = (payload.get("project") or {}).get("name")
    return (
        isinstance(declared, str) and normalise_distribution(declared) == DISTRIBUTION
    )


def runtime_dependencies(root: Path | None = None) -> list[str]:
    return list(runtime_requirements(root or project_root()))


def is_weaver_wheel(filename: str) -> bool:
    return filename.startswith(WHEEL_PREFIX) and filename.endswith(WHEEL_SUFFIX)


def build_wheel(root: Path | None = None, *, output_dir: Path | None = None) -> Path:
    root = root or project_root()
    output_dir = output_dir or (root / "dist")
    before = set(output_dir.glob(f"{WHEEL_PREFIX}*{WHEEL_SUFFIX}"))
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "build",
            "--wheel",
            "--outdir",
            str(output_dir),
            str(root),
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise CommandError(
            "Wheel build failed. Install the `build` package and run Weaver "
            "publish again.\n"
            + (result.stderr.strip() or result.stdout.strip())[-1000:]
        )
    built = sorted(
        set(output_dir.glob(f"{WHEEL_PREFIX}*{WHEEL_SUFFIX}")) - before,
        key=lambda path: path.stat().st_mtime,
    )
    if built:
        return built[-1]
    existing = sorted(
        output_dir.glob(f"{WHEEL_PREFIX}*{WHEEL_SUFFIX}"),
        key=lambda path: path.stat().st_mtime,
    )
    if not existing:
        raise CommandError(
            f"Wheel build produced no {WHEEL_PREFIX}*{WHEEL_SUFFIX} file in "
            f"{output_dir}."
        )
    return existing[-1]


def resolve_environment_owner(
    workspace_name: str | None, environment: EnvironmentRef | str
) -> tuple[str, EnvironmentRef]:
    reference = EnvironmentRef.parse(environment)
    if (
        workspace_name is not None
        and reference.workspace is not None
        and workspace_name != reference.workspace
    ):
        raise CommandError(
            f"Environment {str(reference)!r} belongs to workspace "
            f"{reference.workspace!r}, which conflicts with workspace "
            f"{workspace_name!r}."
        )
    owner = reference.workspace or workspace_name
    if owner is None:
        raise CommandError(
            "An unqualified Environment requires --workspace or workspace "
            "configuration."
        )
    return owner, reference


def find_existing_environment(
    workspace_name: str, environment_name: str, *, client: FabricClient
) -> tuple[WorkspaceItem, Item]:
    workspace = find_workspace(workspace_name, client=client)
    try:
        environment = find_item(
            workspace, environment_name, item_type=ENVIRONMENT, client=client
        )
    except ItemNotFoundError as exc:
        raise CommandError(
            f"Environment {environment_name!r} was not found in workspace "
            f"{workspace.name!r}.\nCreate the Environment in Fabric first, or "
            "publish a local definition with --path, which creates it."
        ) from exc
    return workspace, environment


def _staging_base(environment: Item) -> str:
    return (
        f"workspaces/{environment.workspace_id}/environments/{environment.id}/staging"
    )


def _environment_base(environment: Item) -> str:
    return f"workspaces/{environment.workspace_id}/environments/{environment.id}"


def read_staging(environment: Item, *, client: FabricClient) -> dict:
    libraries = client.paged(
        f"{_staging_base(environment)}/libraries?beta=false",
        key="libraries",
        not_found_empty=True,
    )
    return {"libraries": libraries}


def read_published(environment: Item, *, client: FabricClient) -> dict:
    libraries = client.paged(
        f"{_environment_base(environment)}/libraries?beta=false",
        key="libraries",
        not_found_empty=True,
    )
    return {"libraries": libraries}


def read_published_spark_compute(environment: Item, *, client: FabricClient) -> dict:
    return client.get_json(f"{_environment_base(environment)}/sparkcompute?beta=false")


def library_wheels(libraries: dict) -> list[str]:
    return [
        str(entry.get("name") or "")
        for entry in libraries.get("libraries", ())
        if str(entry.get("libraryType") or "").casefold() == "custom"
    ]


staged_wheels = library_wheels


def read_staging_external_libraries(environment: Item, *, client: FabricClient) -> str:
    """Read published and pending external libraries from staging."""

    path = f"{_staging_base(environment)}/libraries/exportExternalLibraries"
    try:
        response = client.request("GET", path, expected=(200,))
    except FabricError as exc:
        if exc.status_code == 404:
            return ""
        raise
    return response.content.decode("utf-8-sig")


def import_external_libraries(
    environment: Item, text: str, *, client: FabricClient
) -> None:
    """Replace the staged external library list."""

    import requests

    path = f"{_staging_base(environment)}/libraries/importExternalLibraries"
    url = f"{client.api_base_url}/{path}"
    try:
        response = send(
            "POST",
            url,
            headers={
                "Authorization": f"Bearer {client.token}",
                # Fabric rejects multipart uploads here with
                # EnvironmentValidationFailed.
                "Content-Type": "application/octet-stream",
            },
            data=text.encode("utf-8"),
            timeout=client.timeout,
        )
    except requests.exceptions.RequestException as exc:
        raise FabricError(f"POST {url} could not be reached: {exc}") from exc
    if response.status_code not in (200, 201, 202):
        raise FabricError(
            f"importing external libraries returned {response.status_code}: "
            f"{response.text.strip()[:400] or 'no body'}",
            status_code=response.status_code,
        )


def publish_state(environment: Item, *, client: FabricClient) -> str:
    info = client.get_json(_environment_base(environment))
    details = (info.get("properties") or {}).get("publishDetails") or {}
    return details.get("state", "")


def upload_wheel(environment: Item, wheel: Path, *, client: FabricClient) -> None:
    import requests

    path = f"{_staging_base(environment)}/libraries/{quote(wheel.name, safe='')}"
    url = f"{client.api_base_url}/{path}"
    try:
        response = send(
            "POST",
            url,
            headers={
                "Authorization": f"Bearer {client.token}",
                "Content-Type": "application/octet-stream",
            },
            data=wheel.read_bytes(),
            timeout=client.timeout,
        )
    except requests.exceptions.RequestException as exc:
        raise FabricError(f"POST {url} could not be reached: {exc}") from exc
    if response.status_code not in (200, 201, 202):
        raise FabricError(
            f"uploading {wheel.name} returned {response.status_code}: "
            f"{response.text.strip()[:400] or 'no body'}",
            status_code=response.status_code,
        )


def delete_stale_wheels(
    environment: Item,
    keep: str | None,
    staged: list[str],
    *,
    client: FabricClient,
) -> list[str]:
    removed = []
    for filename in staged:
        if filename == keep or not is_weaver_wheel(filename):
            continue
        client.request(
            "DELETE",
            f"{_staging_base(environment)}/libraries/{quote(filename, safe='')}",
            expected=(200, 202, 204),
        )
        removed.append(filename)
    return removed


_TERMINAL_PUBLISH = frozenset({"success", "succeeded", "failed", "cancelled"})


@contextmanager
def _null_step(name: str, detail: str | None = None):
    yield


def _reporter(session):
    return _null_step if session is None else session.step


def publish_and_wait(
    environment: Item,
    *,
    client: FabricClient,
    timeout: float = 1800.0,
    poll_interval: float = 15.0,
) -> str:
    client.request(
        "POST",
        f"{_staging_base(environment)}/publish?beta=false",
        expected=(200, 202),
    )
    deadline = time.time() + timeout
    seen = ""
    while time.time() < deadline:
        seen = publish_state(environment, client=client)
        if seen.casefold() in _TERMINAL_PUBLISH:
            return seen
        time.sleep(poll_interval)
    raise FabricError(
        "Environment publish did not finish within "
        f"{int(timeout)} seconds. Last state: {seen!r}."
    )


def read_definition(
    environment: Item, *, client: FabricClient
) -> EnvironmentDefinition:
    """Read the definition, waiting for Fabric's long-running operation."""

    response = client.request(
        "POST", f"{_environment_base(environment)}/getDefinition", expected=(200, 202)
    )
    if response.status_code == 200:
        return definition_from_payload(response.json())
    operation = response.headers.get("x-ms-operation-id")
    client.wait_for_operation(response)
    return definition_from_payload(
        client.request("GET", f"operations/{operation}/result", expected=(200,)).json()
    )


def update_definition(
    environment: Item, definition: EnvironmentDefinition, *, client: FabricClient
) -> None:
    query = "?updateMetadata=true" if PLATFORM in definition.parts else ""
    response = client.request(
        "POST",
        f"{_environment_base(environment)}/updateDefinition{query}",
        payload={"definition": definition_payload(definition)},
        expected=(200, 202),
    )
    client.wait_for_operation(response)


def create_with_definition(
    workspace: WorkspaceItem,
    name: str,
    definition: EnvironmentDefinition,
    *,
    client: FabricClient,
) -> Item:
    response = client.request(
        "POST",
        f"workspaces/{workspace.id}/environments",
        payload={"displayName": name, "definition": definition_payload(definition)},
        expected=(200, 201, 202),
    )
    client.wait_for_operation(response)
    return find_item(workspace, name, item_type=ENVIRONMENT, client=client)


def _weaver_parts(definition: EnvironmentDefinition) -> tuple[str, ...]:
    return tuple(
        path
        for path in definition.parts
        if path.startswith(CUSTOM_LIBRARIES) and is_weaver_wheel(path.split("/")[-1])
    )


def overlay_weaver(
    definition: EnvironmentDefinition,
    *,
    dev: bool,
    wheel: Path | None = None,
    requirements: tuple[str, ...] = (),
    source: str,
) -> EnvironmentDefinition:
    """Overlay the Weaver libraries for released or development mode.

    Platform metadata, Spark settings, other custom libraries and other external
    requirements are preserved byte for byte.
    """

    parts = dict(definition.parts)
    for path in _weaver_parts(definition):
        parts.pop(path)
    text = definition.external_libraries()
    if dev:
        if wheel is None:
            raise CommandError("A development publication needs a built wheel.")
        parts[EXTERNAL_LIBRARIES] = development_external_libraries(
            text, requirements=requirements, source=source
        ).encode("utf-8")
        parts[f"{CUSTOM_LIBRARIES}{wheel.name}"] = wheel.read_bytes()
    else:
        parts[EXTERNAL_LIBRARIES] = released_external_libraries(
            text, source=source
        ).encode("utf-8")
    return EnvironmentDefinition(parts=parts)


#: Definition parts Fabric stores as text and hands back reformatted.
_TEXT_PARTS = frozenset({PLATFORM, EXTERNAL_LIBRARIES, SPARK_COMPUTE})


def _comparable(definition: EnvironmentDefinition) -> dict:
    """Reduce a definition to the content Fabric preserves.

    A Weaver wheel compares by its path alone, because the filename carries the
    content-addressed version. Every other custom library compares by its bytes.

    Text parts compare as parsed content because Fabric normalises line endings
    and removes quotes around ``runtime_version``.
    """

    weaver = set(_weaver_parts(definition))
    reduced: dict = {}
    for path, content in definition.parts.items():
        if path in weaver:
            reduced[path] = None
        elif path in _TEXT_PARTS:
            reduced[path] = _text_content(content, path)
        else:
            reduced[path] = content
    return reduced


def _text_content(content: bytes, path: str):
    """Parse text without collapsing distinct YAML scalar types."""

    import json

    text = content.decode("utf-8-sig", "replace").replace("\r\n", "\n")
    try:
        if path == PLATFORM:
            return json.dumps(json.loads(text), sort_keys=True)
        import yaml

        loaded = yaml.safe_load(text)
        return json.dumps(_as_fabric_stores(loaded, path), sort_keys=True)
    except Exception:
        # Unparsable content is still content, and a change to it is a change.
        return text


#: Keys Fabric stores as text and returns unquoted, so YAML reads back a number.
#: Only what publishing was observed to do, per part.
_RESTRINGIFIED = {SPARK_COMPUTE: frozenset({"runtime_version"})}


def _as_fabric_stores(value, path: str):
    """Normalise fields whose quoting Fabric does not preserve."""

    fields = _RESTRINGIFIED.get(path)
    if not fields or not isinstance(value, dict):
        return value
    return {
        key: (str(each) if key in fields and each is not None else each)
        for key, each in value.items()
    }


@dataclass
class EnvironmentPublishResult:
    workspace_name: str
    workspace_id: str
    environment_name: str
    environment_id: str
    source_path: str | None
    mode: str
    weaver_requirement: str | None
    wheel_filename: str | None
    removed_wheels: tuple[str, ...]
    action: str
    published: bool
    publish_status: str
    timings: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {
            "workspace_name": self.workspace_name,
            "workspace_id": self.workspace_id,
            "environment_name": self.environment_name,
            "environment_id": self.environment_id,
            "source_path": self.source_path,
            "mode": self.mode,
            "weaver_requirement": self.weaver_requirement,
            "wheel_filename": self.wheel_filename,
            "removed_wheels": list(self.removed_wheels),
            "action": self.action,
            "published": self.published,
            "publish_status": self.publish_status,
            "timings": dict(self.timings),
        }


def _settled(status: str, environment: Item, *, client: FabricClient) -> None:
    """Raise with the failed Spark settings or libraries component."""

    if status.casefold() in {"success", "succeeded"}:
        return
    info = client.get_json(_environment_base(environment))
    details = (info.get("properties") or {}).get("publishDetails") or {}
    components = details.get("componentPublishInfo") or {}
    failed = sorted(
        name
        for name, component in components.items()
        if str((component or {}).get("state") or "").casefold()
        not in {"success", "succeeded"}
    )
    detail = f" Fabric reports {', '.join(failed)} did not settle." if failed else ""
    raise FabricError(f"Environment publish finished with status {status!r}.{detail}")


def publish_environment(
    workspace_name: str | None,
    environment: EnvironmentRef | str | None = None,
    *,
    path: str | Path | None = None,
    dev: bool = False,
    client: FabricClient | None = None,
    root: Path | None = None,
    session=None,
) -> EnvironmentPublishResult:
    """Publish Weaver into a Fabric Environment.

    ``path`` names a local ``*.Environment`` directory, which then supplies both
    the Environment name and its whole definition. Without one, ``environment``
    names an Environment that already exists and only Weaver's own libraries are
    touched.
    """

    client = client or FabricClient()
    step = _reporter(session)
    timings: dict[str, float] = {}

    wheel: Path | None = None
    requirements: tuple[str, ...] = ()
    if dev:
        checkout = root or project_root()
        requirements = runtime_requirements(checkout)
        started = time.perf_counter()
        with step("Build the Weaver wheel"):
            wheel = build_wheel(checkout)
        timings["build"] = round(time.perf_counter() - started, 2)

    if path is not None:
        return _publish_definition(
            workspace_name,
            path,
            dev=dev,
            wheel=wheel,
            requirements=requirements,
            client=client,
            step=step,
            timings=timings,
        )
    if environment is None:
        raise CommandError(
            "Name the Environment to publish into, or pass --path to publish a "
            "local definition."
        )
    return _publish_libraries(
        workspace_name,
        environment,
        dev=dev,
        wheel=wheel,
        requirements=requirements,
        client=client,
        step=step,
        timings=timings,
    )


def _publish_definition(
    workspace_name: str | None,
    path: str | Path,
    *,
    dev: bool,
    wheel: Path | None,
    requirements: tuple[str, ...],
    client: FabricClient,
    step,
    timings: dict,
) -> EnvironmentPublishResult:
    directory = Path(path)
    name = environment_name_from_path(directory)
    local = read_environment_definition(directory)
    desired = overlay_weaver(
        local, dev=dev, wheel=wheel, requirements=requirements, source=str(directory)
    )

    if workspace_name is None:
        raise CommandError(
            "Publishing a local Environment definition requires --workspace or "
            "workspace configuration."
        )

    with step("Find the workspace"):
        workspace = find_workspace(workspace_name, client=client)
    try:
        with step("Find the Environment"):
            item = find_item(workspace, name, item_type=ENVIRONMENT, client=client)
    except ItemNotFoundError:
        item = None

    started = time.perf_counter()
    if item is None:
        with step("Create the Environment", "from the local definition"):
            item = create_with_definition(workspace, name, desired, client=client)
        action = CREATED
    else:
        with step("Read the Environment definition"):
            current = read_definition(item, client=client)
        if _comparable(current) == _comparable(desired) and publish_state(
            item, client=client
        ).casefold() in {"success", "succeeded"}:
            timings["send"] = round(time.perf_counter() - started, 2)
            return _result(
                workspace,
                item,
                source_path=str(directory),
                dev=dev,
                desired=desired,
                removed=(),
                action=UNCHANGED,
                published=False,
                status="AlreadyInstalled",
                timings=timings,
            )
        with step("Update the Environment definition"):
            update_definition(item, desired, client=client)
        action = UPDATED
    timings["send"] = round(time.perf_counter() - started, 2)

    started = time.perf_counter()
    with step("Publish", "Fabric resolves the staged libraries"):
        status = publish_and_wait(item, client=client)
    timings["publish"] = round(time.perf_counter() - started, 2)
    _settled(status, item, client=client)
    return _result(
        workspace,
        item,
        source_path=str(directory),
        dev=dev,
        desired=desired,
        removed=(),
        action=action,
        published=True,
        status=status,
        timings=timings,
    )


def _publish_libraries(
    workspace_name: str | None,
    environment: EnvironmentRef | str,
    *,
    dev: bool,
    wheel: Path | None,
    requirements: tuple[str, ...],
    client: FabricClient,
    step,
    timings: dict,
) -> EnvironmentPublishResult:
    owner_name, reference = resolve_environment_owner(workspace_name, environment)
    with step("Find the Environment"):
        workspace, item = find_existing_environment(
            owner_name, reference.name, client=client
        )

    with step("Read the staged libraries"):
        text = read_staging_external_libraries(item, client=client)
        staged = library_wheels(read_staging(item, client=client))

    source = f"{reference}"
    wanted = (
        development_external_libraries(text, requirements=requirements, source=source)
        if dev
        else released_external_libraries(text, source=source)
    )
    keep = wheel.name if dev and wheel is not None else None
    stale = [name for name in staged if is_weaver_wheel(name) and name != keep]
    upload_needed = keep is not None and keep not in staged
    changed = wanted != text or bool(stale) or upload_needed

    removed: list[str] = []
    started = time.perf_counter()
    if changed:
        with step("Stage Weaver libraries"):
            if wanted != text:
                import_external_libraries(item, wanted, client=client)
            if upload_needed and wheel is not None:
                upload_wheel(item, wheel, client=client)
            removed = delete_stale_wheels(item, keep, staged, client=client)
    timings["send"] = round(time.perf_counter() - started, 2)

    if not changed and publish_state(item, client=client).casefold() in {
        "success",
        "succeeded",
    }:
        return _result(
            workspace,
            item,
            source_path=None,
            dev=dev,
            desired=None,
            removed=(),
            action=UNCHANGED,
            published=False,
            status="AlreadyInstalled",
            requirement=weaver_requirement(_pip_entries(wanted, source=source)),
            wheel_filename=keep,
            timings=timings,
        )

    started = time.perf_counter()
    with step("Publish", "Fabric resolves the staged libraries"):
        status = publish_and_wait(item, client=client)
    timings["publish"] = round(time.perf_counter() - started, 2)
    _settled(status, item, client=client)
    return _result(
        workspace,
        item,
        source_path=None,
        dev=dev,
        desired=None,
        removed=tuple(removed),
        action=UPDATED,
        published=True,
        status=status,
        requirement=weaver_requirement(_pip_entries(wanted, source=source)),
        wheel_filename=keep,
        timings=timings,
    )


def _result(
    workspace,
    item,
    *,
    source_path,
    dev,
    desired,
    removed,
    action,
    published,
    status,
    timings,
    requirement=None,
    wheel_filename=None,
) -> EnvironmentPublishResult:
    if desired is not None:
        entries = _pip_entries(desired.external_libraries(), source="definition")
        requirement = weaver_requirement(entries)
        wheels = [path.split("/")[-1] for path in _weaver_parts(desired)]
        wheel_filename = wheels[0] if wheels else None
    return EnvironmentPublishResult(
        workspace_name=workspace.name,
        workspace_id=workspace.id,
        environment_name=item.name,
        environment_id=item.id,
        source_path=source_path,
        mode=DEVELOPMENT if dev else RELEASED,
        weaver_requirement=requirement,
        wheel_filename=wheel_filename,
        removed_wheels=tuple(removed),
        action=action,
        published=published,
        publish_status=status,
        timings=timings,
    )

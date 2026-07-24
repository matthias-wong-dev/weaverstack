"""Installing the Weaver runtime into a Fabric Environment.

This is the authoritative deployment path: a wheel built from the current
checkout is uploaded to a named Fabric Environment as a custom library, the
external packages Weaver needs are staged from ``environment.yml``, and the
Environment is published. Afterwards a notebook, a Livy session or a Fabric
pytest run attached to that Environment can simply ``import weaver`` — no source
copied into a Lakehouse, no ``sys.path`` surgery.

The command runs from a developer's checkout, not from installed Weaver: it
builds the wheel and reads the Environment definition from the working tree.
"""

from __future__ import annotations

import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

from ..errors import CommandError
from .client import FabricClient, FabricError
from .resources import (
    ENVIRONMENT,
    Item,
    ItemNotFoundError,
    Workspace,
    find_item,
    find_workspace,
)

#: The wheel filenames this deployment owns. Stale copies matching it are
#: replaced; anything else in the Environment is left untouched.
DISTRIBUTION = "weaverstack"
WHEEL_PREFIX = f"{DISTRIBUTION}-"
WHEEL_SUFFIX = ".whl"

#: Where the Environment definition lives, relative to the project root.
ENVIRONMENT_DEFINITION = Path("deployment/fabric/environment.yml")


def project_root() -> Path:
    """The checkout root — the nearest ancestor holding ``pyproject.toml``.

    The install command is a desktop developer tool, so it always runs from a
    source tree. Locating the root from the package file keeps it working
    whatever directory the command is invoked from.
    """

    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "pyproject.toml").is_file():
            return parent
    raise CommandError(
        "cannot find the project root (no pyproject.toml above "
        f"{here}); run weaver install from a Weaver checkout"
    )


def _normalise(name: str) -> str:
    """A PEP 503 distribution name, stripped of any version specifier."""

    bare = re.split(r"[<>=!~;\[\s]", name.strip(), maxsplit=1)[0]
    return re.sub(r"[-_.]+", "-", bare).lower()


def runtime_dependencies(root: Path | None = None) -> list[str]:
    """The packages installed Weaver needs, from ``[project].dependencies``."""

    import tomllib

    root = root or project_root()
    data = tomllib.loads((root / "pyproject.toml").read_text("utf-8"))
    return list(data.get("project", {}).get("dependencies", []))


def environment_dependencies(root: Path | None = None) -> list[str]:
    """The pip packages named in ``environment.yml``."""

    import yaml

    root = root or project_root()
    text = (root / ENVIRONMENT_DEFINITION).read_text("utf-8")
    document = yaml.safe_load(text) or {}
    pip: list[str] = []
    for entry in document.get("dependencies", []):
        if isinstance(entry, dict) and "pip" in entry:
            pip.extend(entry["pip"] or [])
    return pip


def missing_from_environment(root: Path | None = None) -> list[str]:
    """Runtime dependencies that ``environment.yml`` fails to install.

    The check that keeps the two definitions from drifting: a package added to
    ``pyproject.toml`` but not to the Environment would be absent in Fabric.
    """

    root = root or project_root()
    staged = {_normalise(name) for name in environment_dependencies(root)}
    return [
        dependency
        for dependency in runtime_dependencies(root)
        if _normalise(dependency) not in staged
    ]


# --- building the wheel ------------------------------------------------------


def is_weaver_wheel(filename: str) -> bool:
    """Whether a filename is a Weaver distribution wheel, and only that.

    The guard on every delete: an Environment may hold other custom libraries,
    and this deployment owns exactly the ``weaverstack-*.whl`` it uploads.
    """

    return filename.startswith(WHEEL_PREFIX) and filename.endswith(WHEEL_SUFFIX)


def build_wheel(root: Path | None = None, *, output_dir: Path | None = None) -> Path:
    """Build a wheel from the checkout and return its exact path.

    The version is git-derived (see pyproject), so a changed tree produces a
    differently-named wheel without anyone editing a version string.
    """

    root = root or project_root()
    output_dir = output_dir or (root / "dist")
    before = set(output_dir.glob(f"{WHEEL_PREFIX}*{WHEEL_SUFFIX}"))
    result = subprocess.run(
        [sys.executable, "-m", "build", "--wheel", "--outdir", str(output_dir), str(root)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise CommandError(
            "wheel build failed — is the [cli] extra installed?\n"
            + (result.stderr.strip() or result.stdout.strip())[-1000:]
        )
    built = sorted(
        set(output_dir.glob(f"{WHEEL_PREFIX}*{WHEEL_SUFFIX}")) - before,
        key=lambda p: p.stat().st_mtime,
    )
    if built:
        return built[-1]
    # A rebuild of an unchanged, already-built version produces no new file.
    existing = sorted(
        output_dir.glob(f"{WHEEL_PREFIX}*{WHEEL_SUFFIX}"), key=lambda p: p.stat().st_mtime
    )
    if not existing:
        raise CommandError(f"wheel build produced no {WHEEL_PREFIX}*{WHEEL_SUFFIX} in {output_dir}")
    return existing[-1]


# --- the Fabric Environment --------------------------------------------------


def find_or_create_environment(
    workspace: Workspace, name: str, *, client: FabricClient
) -> tuple[Item, bool]:
    """The named Environment, created if it does not yet exist.

    Idempotent: a second call with the same workspace and name returns the same
    item rather than a suffixed duplicate. Returns ``(item, created)``.
    """

    try:
        return find_item(workspace, name, item_type=ENVIRONMENT, client=client), False
    except ItemNotFoundError:
        pass

    response = client.request(
        "POST",
        f"workspaces/{workspace.id}/environments",
        payload={"displayName": name, "description": "Weaver runtime"},
        expected=(200, 201, 202),
    )
    if response.status_code == 202:
        item = _await_environment(workspace, name, client=client)
    else:
        body = response.json()
        item = Item(id=body["id"], name=name, type=ENVIRONMENT, workspace_id=workspace.id)
    return item, True


def _await_environment(
    workspace: Workspace, name: str, *, client: FabricClient, timeout: float = 120.0
) -> Item:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            return find_item(workspace, name, item_type=ENVIRONMENT, client=client)
        except ItemNotFoundError:
            time.sleep(3.0)
    raise FabricError(f"Environment {name!r} did not appear within {int(timeout)}s")


def _staging_base(env: Item) -> str:
    return f"workspaces/{env.workspace_id}/environments/{env.id}/staging"


def read_staging(env: Item, *, client: FabricClient) -> dict:
    """What the Environment currently has staged: custom wheels and the env yml."""

    return client.get_json(f"{_staging_base(env)}/libraries")


def read_published(env: Item, *, client: FabricClient) -> dict:
    """What the Environment has *published* — the live image's libraries.

    The diff that decides whether a republish is needed compares against this,
    not against staging: staging can hold half-finished changes from an
    interrupted run, whereas the published revision is what a session actually
    imports. A never-published Environment answers 404, read as "nothing".
    """

    try:
        return client.get_json(
            f"workspaces/{env.workspace_id}/environments/{env.id}/libraries"
        )
    except FabricError as exc:
        if exc.status_code == 404:
            return {}
        raise


def library_wheels(libraries: dict) -> list[str]:
    custom = (libraries.get("customLibraries") or {}).get("wheelFiles") or []
    return list(custom)


#: Backwards-compatible alias — reads wheels out of a staging or published body.
staged_wheels = library_wheels


def publish_state(env: Item, *, client: FabricClient) -> str:
    """The Environment's last publish outcome, e.g. ``Success`` or ``Running``."""

    info = client.get_json(f"workspaces/{env.workspace_id}/environments/{env.id}")
    details = (info.get("properties") or {}).get("publishDetails") or {}
    return details.get("state", "")


def upload_wheel(env: Item, wheel: Path, *, client: FabricClient) -> None:
    """Upload one wheel's exact bytes to Environment staging."""

    import requests

    url = f"{client.api_base_url}/{_staging_base(env)}/libraries"
    response = requests.post(
        url,
        headers={"Authorization": f"Bearer {client.token}"},
        files={"file": (wheel.name, wheel.read_bytes(), "application/octet-stream")},
        timeout=client.timeout,
    )
    if response.status_code not in (200, 201):
        raise FabricError(
            f"uploading {wheel.name} returned {response.status_code}: "
            f"{response.text.strip()[:400] or 'no body'}",
            status_code=response.status_code,
        )


def upload_environment_yml(env: Item, definition: Path, *, client: FabricClient) -> None:
    """Stage the external-dependency definition as the Environment's yml."""

    import requests

    url = f"{client.api_base_url}/{_staging_base(env)}/libraries"
    response = requests.post(
        url,
        headers={"Authorization": f"Bearer {client.token}"},
        files={"file": ("environment.yml", definition.read_bytes(), "application/octet-stream")},
        timeout=client.timeout,
    )
    if response.status_code not in (200, 201):
        raise FabricError(
            f"uploading environment.yml returned {response.status_code}: "
            f"{response.text.strip()[:400] or 'no body'}",
            status_code=response.status_code,
        )


def delete_stale_wheels(env: Item, keep: str, staged: list[str], *, client: FabricClient) -> list[str]:
    """Remove staged Weaver wheels other than ``keep``. Returns what was removed.

    Only ``weaverstack-*.whl`` is ever deleted, so an unrelated custom library
    an operator added to the Environment is never touched.
    """

    removed = []
    for filename in staged:
        if filename == keep or not is_weaver_wheel(filename):
            continue
        client.request(
            "DELETE",
            f"{_staging_base(env)}/libraries?libraryToDelete={filename}",
            expected=(200, 202, 204),
        )
        removed.append(filename)
    return removed


#: Publish is complete at one of these states; anything else is still running.
_TERMINAL_PUBLISH = frozenset({"success", "succeeded", "failed", "cancelled"})


def publish_and_wait(
    env: Item,
    *,
    client: FabricClient,
    timeout: float = 1800.0,
    poll_interval: float = 15.0,
) -> str:
    """Publish the staged Environment and poll until it settles.

    Returns the terminal state. Publication is where Fabric resolves the pip
    dependencies into the image, so it is the slow step and the one that decides
    whether ``import weaver`` will work.
    """

    client.request("POST", f"{_staging_base(env)}/publish", expected=(200, 202))
    deadline = time.time() + timeout
    while time.time() < deadline:
        state = publish_state(env, client=client)
        if state.lower() in _TERMINAL_PUBLISH:
            return state
        time.sleep(poll_interval)
    raise FabricError(f"publish did not finish within {int(timeout)}s (last state polled)")


# --- the orchestrated install ------------------------------------------------


@dataclass
class InstallResult:
    """What one ``weaver install`` did — serialisable for ``--json``."""

    workspace_name: str
    workspace_id: str
    environment_name: str
    environment_id: str
    package_name: str
    package_version: str
    wheel_filename: str
    created_environment: bool
    dependencies_changed: bool
    wheel_changed: bool
    published: bool
    publish_status: str
    timings: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        data = self.__dict__.copy()
        return data


def _version_from_wheel(filename: str) -> str:
    # weaverstack-<version>-py3-none-any.whl
    stem = filename[len(WHEEL_PREFIX):-len(WHEEL_SUFFIX)]
    return stem.split("-py3-")[0].split("-py2.py3-")[0]


def install(
    workspace_name: str,
    environment_name: str,
    *,
    publish: bool = True,
    client: FabricClient | None = None,
    root: Path | None = None,
) -> InstallResult:
    """Build the wheel, stage what changed, and publish only if needed.

    The one supported installation path. The wanted wheel and dependencies are
    diffed against the Environment's *published* revision: an ordinary code
    change replaces only the wheel, an unchanged dependency set is left alone,
    and a rerun that changes nothing (same source — the version is stable) does
    not republish at all.
    """

    root = root or project_root()
    client = client or FabricClient()
    timings: dict[str, float] = {}

    t = time.perf_counter()
    wheel = build_wheel(root)
    timings["build"] = time.perf_counter() - t
    version = _version_from_wheel(wheel.name)

    workspace = find_workspace(workspace_name, client=client)
    env, created = find_or_create_environment(workspace, environment_name, client=client)

    definition_path = root / ENVIRONMENT_DEFINITION
    wanted_yml = definition_path.read_text("utf-8")

    # Diff against what is *published* (what a session imports), not staging.
    published_libs = read_published(env, client=client)
    deps_changed = wanted_yml.strip() != (published_libs.get("environmentYml") or "").strip()
    wheel_changed = wheel.name not in library_wheels(published_libs)

    # Stage only the differences, and only if they are not already staged (an
    # interrupted earlier run may have staged them).
    staging = read_staging(env, client=client)
    staged = library_wheels(staging)
    t = time.perf_counter()
    if deps_changed and wanted_yml.strip() != (staging.get("environmentYml") or "").strip():
        upload_environment_yml(env, definition_path, client=client)
    if wheel_changed and wheel.name not in staged:
        upload_wheel(env, wheel, client=client)
        delete_stale_wheels(env, wheel.name, staged, client=client)
    timings["upload"] = time.perf_counter() - t

    state = publish_state(env, client=client)
    already_published = state.lower() in {"success", "succeeded"}
    something_changed = created or deps_changed or wheel_changed
    published_now = False
    if not publish:
        publish_status = "Skipped"
    elif not something_changed and already_published:
        publish_status = "AlreadyInstalled"
        published_now = True
    else:
        t = time.perf_counter()
        publish_status = publish_and_wait(env, client=client)
        timings["publish"] = time.perf_counter() - t
        published_now = publish_status.lower() in {"success", "succeeded"}
        if not published_now:
            raise FabricError(f"Environment publish finished as {publish_status!r}, not Success")

    return InstallResult(
        workspace_name=workspace.name,
        workspace_id=workspace.id,
        environment_name=env.name,
        environment_id=env.id,
        package_name=DISTRIBUTION,
        package_version=version,
        wheel_filename=wheel.name,
        created_environment=created,
        dependencies_changed=deps_changed,
        wheel_changed=wheel_changed,
        published=published_now,
        publish_status=publish_status,
        timings={k: round(v, 2) for k, v in timings.items()},
    )

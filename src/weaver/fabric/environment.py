"""Build and publish the Weaver wheel to a Fabric Environment.

The install command stages the wheel and runtime dependencies from a checkout,
then publishes the selected Environment.
"""

from __future__ import annotations

import re
import subprocess
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

from ..errors import CommandError
from .client import FabricClient, FabricError
from .resources import (
    ENVIRONMENT,
    Item,
    ItemNotFoundError,
    WorkspaceItem,
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
    """Return the nearest ancestor containing ``pyproject.toml``."""

    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "pyproject.toml").is_file():
            return parent
    raise CommandError(
        "A Weaver project root was not found. Run `weaver install` from a "
        f"checkout containing pyproject.toml above {here}."
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


#: Packages a desktop needs to reach Fabric, and Fabric itself never imports.
#: They are ordinary runtime dependencies — `pip install weaverstack` gives you
#: a working CLI — but installing them *into* a Fabric Environment would ship
#: an HTTP stack, a build frontend and a readline shim to a Spark image that
#: has no use for any of them.
DESKTOP_ONLY = frozenset({"azure-identity", "requests", "build", "pyreadline3"})


def missing_from_environment(root: Path | None = None) -> list[str]:
    """Runtime dependencies that ``environment.yml`` fails to install.

    The check that keeps the two definitions from drifting: a package added to
    ``pyproject.toml`` but not to the Environment would be absent in Fabric.
    Desktop-only packages are excluded by name rather than by guesswork, so
    adding one to that set is a visible decision.
    """

    root = root or project_root()
    staged = {_normalise(name) for name in environment_dependencies(root)}
    return [
        dependency
        for dependency in runtime_dependencies(root)
        if _normalise(dependency) not in staged
        and _normalise(dependency) not in {_normalise(n) for n in DESKTOP_ONLY}
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
            "Wheel build failed. Install the [cli] extra and try again.\n"
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
        raise CommandError(
            f"Wheel build produced no {WHEEL_PREFIX}*{WHEEL_SUFFIX} file in {output_dir}."
        )
    return existing[-1]


# --- the Fabric Environment --------------------------------------------------


def find_or_create_environment(
    workspace: WorkspaceItem, name: str, *, client: FabricClient
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
    workspace: WorkspaceItem, name: str, *, client: FabricClient, timeout: float = 120.0
) -> Item:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            return find_item(workspace, name, item_type=ENVIRONMENT, client=client)
        except ItemNotFoundError:
            time.sleep(3.0)
    raise FabricError(f"Environment {name!r} did not appear within {int(timeout)} seconds.")


def _staging_base(env: Item) -> str:
    return f"workspaces/{env.workspace_id}/environments/{env.id}/staging"


def read_staging(env: Item, *, client: FabricClient) -> dict:
    """What the Environment currently has staged: custom wheels and the env yml.

    A freshly created Environment answers 404 here — ``This environment does not
    have any staged libraries`` — which is the same "nothing yet"
    :func:`read_published` reads from a 404, and is read the same way. Treated
    as fatal, it made the first ``weaver install`` into a new Environment fail.
    """

    try:
        return client.get_json(f"{_staging_base(env)}/libraries")
    except FabricError as exc:
        if exc.status_code == 404:
            return {}
        raise


def read_published(env: Item, *, client: FabricClient) -> dict:
    """What the Environment has *published* — the live image's libraries.

    The diff that decides whether a republish is needed compares against this
    rather than staging, which can hold half-finished changes from an
    interrupted run. A never-published Environment answers 404, read as
    "nothing".
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


@contextmanager
def _null_step(name: str, detail: str | None = None):
    yield


def _reporter(session):
    """``session.step`` when there is a Session, and a no-op otherwise.

    Reporting is the caller's: a pytest fixture installing Weaver wants no
    frames and the CLI wants them. One accessor rather than an ``if session`` at
    every phase.
    """

    return _null_step if session is None else session.step


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
    seen = ""
    while time.time() < deadline:
        seen = publish_state(env, client=client)
        if seen.lower() in _TERMINAL_PUBLISH:
            return seen
        time.sleep(poll_interval)
    # Name the state last seen rather than describing it: an empty string, from
    # Fabric answering with no publishDetails, reads very differently from a
    # publish genuinely stuck in Running.
    raise FabricError(
        f"Environment publish did not finish within {int(timeout)} seconds. Last state: {seen!r}."
    )


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
    client: FabricClient | None = None,
    root: Path | None = None,
    session=None,
) -> InstallResult:
    """Build the wheel, stage what changed, and publish if anything changed.

    The one installation path, and it always finishes: there is no
    stage-without-publish mode, because staging is Fabric's scratch area and a
    session imports the published revision.

    The wanted wheel and dependencies are diffed against the published revision,
    so a code change replaces only the wheel, an unchanged dependency set is
    left alone, and a rerun over the same source does not republish.

    ``session`` is used only for reporting. Publishing is minutes of waiting, so
    the steps are framed rather than silent.
    """

    root = root or project_root()
    client = client or FabricClient()
    timings: dict[str, float] = {}
    step = _reporter(session)

    t = time.perf_counter()
    with step("Build the wheel"):
        wheel = build_wheel(root)
    timings["build"] = time.perf_counter() - t
    version = _version_from_wheel(wheel.name)

    with step("Find the Environment"):
        workspace = find_workspace(workspace_name, client=client)
        env, created = find_or_create_environment(workspace, environment_name, client=client)

    definition_path = root / ENVIRONMENT_DEFINITION
    wanted_yml = definition_path.read_text("utf-8")

    # Diff against what is *published* (what a session imports), not staging.
    with step("Read the published revision"):
        published_libs = read_published(env, client=client)
        deps_changed = wanted_yml.strip() != (published_libs.get("environmentYml") or "").strip()
        wheel_changed = wheel.name not in library_wheels(published_libs)

        # Stage only the differences, and only if they are not already staged (an
        # interrupted earlier run may have staged them).
        staging = read_staging(env, client=client)
        staged = library_wheels(staging)

    t = time.perf_counter()
    with step("Stage what changed"):
        if deps_changed and wanted_yml.strip() != (staging.get("environmentYml") or "").strip():
            upload_environment_yml(env, definition_path, client=client)
        if wheel_changed and wheel.name not in staged:
            upload_wheel(env, wheel, client=client)
            delete_stale_wheels(env, wheel.name, staged, client=client)
    timings["upload"] = time.perf_counter() - t

    state = publish_state(env, client=client)
    already_published = state.lower() in {"success", "succeeded"}
    something_changed = created or deps_changed or wheel_changed
    if not something_changed and already_published:
        publish_status = "AlreadyInstalled"
        published_now = True
    else:
        t = time.perf_counter()
        with step("Publish", "Fabric resolves dependencies into the image"):
            publish_status = publish_and_wait(env, client=client)
        timings["publish"] = time.perf_counter() - t
        published_now = publish_status.lower() in {"success", "succeeded"}
        if not published_now:
            raise FabricError(f"Environment publish finished with status {publish_status!r}.")

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

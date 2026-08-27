"""Install Weaver into an existing Fabric Environment.

Publication reuses compatible published packages, adds absent requirements as
custom wheels, and replaces Weaver wheels. Other Environment content is kept.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import quote

from ..errors import CommandError
from ..workspaces import EnvironmentRef
from .client import FabricClient, FabricError, send
from .environment_packages import (
    EnvironmentPackageConflict,
    ResolvedRequirement,
    custom_library_names,
    plan_requirements,
    resolve_wheel_closure,
    runtime_requirements,
)
from .resources import (
    ENVIRONMENT,
    Item,
    ItemNotFoundError,
    WorkspaceItem,
    find_item,
    find_workspace,
)

DISTRIBUTION = "weaverstack"
WHEEL_PREFIX = f"{DISTRIBUTION}-"
WHEEL_SUFFIX = ".whl"


def project_root() -> Path:
    """Return the nearest ancestor containing ``pyproject.toml``."""

    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "pyproject.toml").is_file():
            return parent
    raise CommandError(
        "A Weaver project root was not found. Run `weaver fabric environment "
        "publish` from a checkout containing pyproject.toml."
    )


def runtime_dependencies(root: Path | None = None) -> list[str]:
    """Return Weaver's Fabric requirements from ``pyproject.toml``."""

    return list(runtime_requirements(root or project_root()))


def is_weaver_wheel(filename: str) -> bool:
    """Return whether Weaver owns this custom-library filename."""

    return filename.startswith(WHEEL_PREFIX) and filename.endswith(WHEEL_SUFFIX)


def build_wheel(root: Path | None = None, *, output_dir: Path | None = None) -> Path:
    """Build a wheel from the checkout and return its exact path."""

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
    """Resolve an Environment reference and reject conflicting workspaces."""

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
    """Resolve an existing Environment and report the provisioning action."""

    workspace = find_workspace(workspace_name, client=client)
    try:
        environment = find_item(
            workspace, environment_name, item_type=ENVIRONMENT, client=client
        )
    except ItemNotFoundError as exc:
        raise CommandError(
            f"Environment {environment_name!r} was not found in workspace "
            f"{workspace.name!r}.\nCreate the Environment in Fabric first, "
            "configure any packages you require, publish it, then run Weaver "
            "publish again."
        ) from exc
    return workspace, environment


def _staging_base(environment: Item) -> str:
    return (
        f"workspaces/{environment.workspace_id}/environments/{environment.id}/staging"
    )


def _environment_base(environment: Item) -> str:
    return f"workspaces/{environment.workspace_id}/environments/{environment.id}"


def read_staging(environment: Item, *, client: FabricClient) -> dict:
    """Return the Environment's GA staging library list."""

    try:
        return client.get_json(f"{_staging_base(environment)}/libraries?beta=false")
    except FabricError as exc:
        if exc.status_code == 404:
            return {}
        raise


def read_published(environment: Item, *, client: FabricClient) -> dict:
    """Return the Environment's GA published library list."""

    try:
        return client.get_json(f"{_environment_base(environment)}/libraries?beta=false")
    except FabricError as exc:
        if exc.status_code == 404:
            return {}
        raise


def library_wheels(libraries: dict) -> list[str]:
    """Return Custom library names from a GA library response."""

    return list(custom_library_names(libraries))


staged_wheels = library_wheels


def publish_state(environment: Item, *, client: FabricClient) -> str:
    """Return the Environment's last publish state."""

    info = client.get_json(_environment_base(environment))
    details = (info.get("properties") or {}).get("publishDetails") or {}
    return details.get("state", "")


def upload_wheel(environment: Item, wheel: Path, *, client: FabricClient) -> None:
    """Upload one custom wheel through the GA staging endpoint."""

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
    keep: str,
    staged: list[str],
    *,
    client: FabricClient,
) -> list[str]:
    """Remove staged Weaver wheels other than ``keep``."""

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
    """Publish staged libraries and return the terminal state."""

    client.request("POST", f"{_staging_base(environment)}/publish", expected=(200, 202))
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


@dataclass
class EnvironmentPublishResult:
    """What one Environment publication did, serialisable for the CLI."""

    workspace_name: str
    workspace_id: str
    environment_name: str
    environment_id: str
    package_name: str
    package_version: str
    wheel_filename: str
    requirements: tuple[ResolvedRequirement, ...]
    staged_dependency_wheels: tuple[str, ...]
    reused_requirements: tuple[str, ...]
    wheel_changed: bool
    published: bool
    publish_status: str
    timings: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {
            "workspace_name": self.workspace_name,
            "workspace_id": self.workspace_id,
            "environment_name": self.environment_name,
            "environment_id": self.environment_id,
            "package_name": self.package_name,
            "package_version": self.package_version,
            "wheel_filename": self.wheel_filename,
            "requirements": [item.as_dict() for item in self.requirements],
            "staged_dependency_wheels": list(self.staged_dependency_wheels),
            "reused_requirements": list(self.reused_requirements),
            "wheel_changed": self.wheel_changed,
            "published": self.published,
            "publish_status": self.publish_status,
            "timings": dict(self.timings),
        }


def _version_from_wheel(filename: str) -> str:
    from packaging.utils import parse_wheel_filename

    _name, version, _build, _tags = parse_wheel_filename(filename)
    return str(version)


def _conflict_message(
    reference: EnvironmentRef, conflict: EnvironmentPackageConflict
) -> str:
    lines = [f"Cannot publish Weaver into {str(reference)!r}.", ""]
    for item in conflict.conflicts:
        supplied = ", ".join(item.provided) or "an unknown version"
        required = item.required or "any version"
        lines.extend(
            [
                f"{item.location.capitalize()} provides:",
                f"  {item.name}=={supplied}",
                "Weaver requires:",
                f"  {item.name}{required}",
                "",
            ]
        )
    lines.extend(
        [
            "Resolve the package version in the Fabric Environment and publish it,",
            "then run Weaver publish again. No Environment changes were staged.",
        ]
    )
    return "\n".join(lines)


def publish_environment(
    workspace_name: str | None,
    environment: EnvironmentRef | str,
    *,
    client: FabricClient | None = None,
    root: Path | None = None,
    session=None,
) -> EnvironmentPublishResult:
    """Install or update Weaver in an existing Fabric Environment."""

    root = root or project_root()
    client = client or FabricClient()
    owner_name, reference = resolve_environment_owner(workspace_name, environment)
    step = _reporter(session)
    timings: dict[str, float] = {}

    with step("Find the Environment"):
        workspace, item = find_existing_environment(
            owner_name, reference.name, client=client
        )

    with step("Read Environment libraries"):
        published = read_published(item, client=client)
        staging = read_staging(item, client=client)

    requirements = runtime_requirements(root)
    with tempfile.TemporaryDirectory(prefix="weaver-fabric-wheels-") as temporary:
        t = time.perf_counter()
        with step("Resolve Weaver requirements"):
            closure = resolve_wheel_closure(requirements, Path(temporary))
            try:
                package_plan = plan_requirements(
                    closure, published=published, staging=staging
                )
            except EnvironmentPackageConflict as exc:
                raise CommandError(_conflict_message(reference, exc)) from exc
        timings["resolve"] = time.perf_counter() - t

        t = time.perf_counter()
        with step("Build the Weaver wheel"):
            wheel = build_wheel(root)
        timings["build"] = time.perf_counter() - t

        published_custom = set(library_wheels(published))
        staged_custom = set(library_wheels(staging))
        published_weaver = {name for name in published_custom if is_weaver_wheel(name)}
        staged_weaver = {name for name in staged_custom if is_weaver_wheel(name)}
        wheel_changed = wheel.name not in published_weaver
        stale_weaver = (published_weaver | staged_weaver) - {wheel.name}
        related_change = (
            package_plan.needs_publish or wheel_changed or bool(stale_weaver)
        )

        uploaded_dependencies: list[str] = []
        t = time.perf_counter()
        if related_change:
            with step("Stage Weaver libraries"):
                for dependency in package_plan.upload:
                    upload_wheel(item, dependency.path, client=client)
                    uploaded_dependencies.append(dependency.filename)
                if wheel.name not in staged_custom:
                    upload_wheel(item, wheel, client=client)
                delete_stale_wheels(
                    item,
                    wheel.name,
                    sorted(staged_custom | published_custom | {wheel.name}),
                    client=client,
                )
        timings["upload"] = time.perf_counter() - t

    if related_change:
        t = time.perf_counter()
        with step("Publish", "Fabric resolves the staged libraries"):
            status = publish_and_wait(item, client=client)
        timings["publish"] = time.perf_counter() - t
        if status.casefold() not in {"success", "succeeded"}:
            raise FabricError(f"Environment publish finished with status {status!r}.")
        published_now = True
    else:
        status = "AlreadyInstalled"
        published_now = False

    return EnvironmentPublishResult(
        workspace_name=workspace.name,
        workspace_id=workspace.id,
        environment_name=item.name,
        environment_id=item.id,
        package_name=DISTRIBUTION,
        package_version=_version_from_wheel(wheel.name),
        wheel_filename=wheel.name,
        requirements=package_plan.requirements,
        staged_dependency_wheels=tuple(uploaded_dependencies),
        reused_requirements=package_plan.reused,
        wheel_changed=wheel_changed,
        published=published_now,
        publish_status=status,
        timings={key: round(value, 2) for key, value in timings.items()},
    )

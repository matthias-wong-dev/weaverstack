"""Create a Weaver project and its declared Fabric items.

Environment publication is optional. Examples are source files for a later
build, load and test. The first build creates the catalogue tables.
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .declaration.model import LAKEHOUSE, WAREHOUSE
from .errors import WeaverError
from .onboarding import (
    WORKSPACE_CONFIG_FILE,
    ProjectRequest,
    environment_definition_files,
    example_files,
    project_files,
)
from .onboarding.environment import environment_directory

#: The names a caller may leave to the defaults.
DEFAULT_CATALOGUE = "Catalogue"
DEFAULT_ENVIRONMENT = "Weaver"

#: What this run did to one Fabric item. The CLI turns these into display text.
CREATED = "created"
EXISTING = "existing"
PLANNED = "planned"
UNCHANGED = "unchanged"

#: What each requested item is to the project.
CATALOGUE_ROLE = "Catalogue"
ENVIRONMENT_ROLE = "Environment"


class InitialiseError(WeaverError):
    """Raised when a project cannot be set up as it was asked for."""


@dataclass(frozen=True)
class FabricItemOutcome:
    """One requested Fabric item and its creation or reuse outcome."""

    role: str
    name: str
    status: str
    action: str | None = None

    def to_mapping(self) -> dict[str, str | None]:
        return {
            "role": self.role,
            "name": self.name,
            "status": self.status,
            "action": self.action,
        }


@dataclass(frozen=True)
class ExampleOutcome:
    """Whether Sales example source was generated."""

    generated: bool = False

    def to_mapping(self) -> dict[str, Any]:
        return {"generated": self.generated}


@dataclass(frozen=True)
class InitialiseReport:
    """What one initialise run set up, for a caller to print or assert on."""

    repository: str
    workspace: str
    resources: tuple[FabricItemOutcome, ...] = ()
    files: tuple[str, ...] = ()
    example: ExampleOutcome = field(default_factory=ExampleOutcome)
    dry_run: bool = False
    environment_publication: str = "deferred"
    environment_definition: str = "written"

    @property
    def created(self) -> tuple[str, ...]:
        """Every Fabric item this run created."""

        return tuple(
            f"{outcome.role}/{outcome.name}"
            for outcome in self.resources
            if CREATED in (outcome.status, outcome.action)
        )

    @property
    def succeeded(self) -> bool:
        return True

    @property
    def next_commands(self) -> tuple[str, ...]:
        """What to run next, from the project's own directory."""

        return ("weaver build", "weaver load", "weaver test")

    def to_mapping(self) -> dict[str, Any]:
        """A plain structure, for a CLI to serialise. The CLI owns no semantics."""

        return {
            "repository": self.repository,
            "workspace": self.workspace,
            "resources": [outcome.to_mapping() for outcome in self.resources],
            "files": list(self.files),
            "example": self.example.to_mapping(),
            "example_added": self.example.generated,
            "environment_publication": self.environment_publication,
            "environment_definition": self.environment_definition,
            "dry_run": self.dry_run,
            "next_commands": list(self.next_commands),
        }


# --- the operation -------------------------------------------------------------


def initialise(
    repository,
    *,
    workspace: str | None = None,
    catalogue: str = DEFAULT_CATALOGUE,
    environment: str = DEFAULT_ENVIRONMENT,
    lakehouse: str | None = None,
    warehouse: str | None = None,
    example: bool = False,
    publish_environment: bool = False,
    install_weaver: bool | None = None,
    dry_run: bool = False,
    session=None,
    client=None,
) -> InitialiseReport:
    """Create the project and requested Fabric items after destination validation.

    ``example`` adds source files. ``publish_environment`` publishes the local
    Environment definition after the project has been written.
    """

    if repository is None:
        raise InitialiseError("A project folder is required.")
    if install_weaver is not None:
        import warnings

        warnings.warn(
            "install_weaver is deprecated. Use publish_environment.",
            DeprecationWarning,
            stacklevel=2,
        )
        publish_environment = publish_environment or install_weaver
    destination = Path(repository).resolve()
    request = ProjectRequest(
        workspace=_workspace_name(workspace, session=session),
        catalogue=catalogue,
        environment=environment,
        lakehouse=lakehouse,
        warehouse=warehouse,
        example=example,
    )

    from .sessions.host import use_or_create_session

    addressed = _bare(request)
    resources = []
    with use_or_create_session(session, workspace=addressed) as opened:
        with opened.task("Setting up your Weaver project", request.workspace):
            with opened.step("Checking the workspace", request.workspace):
                # The Session's client carries its identity and records REST telemetry.
                rest = (
                    client if client is not None else opened.resolver(addressed).client
                )
                state, found, physical, environment_item = _read_the_workspace(
                    request, client=rest
                )

            with opened.step("Reading the Environment", request.environment):
                files = _generated_files(request)
                definition_status = "written"
                directory = environment_directory(request.environment)
                if state != MISSING and not (destination / directory).exists():
                    definition_status = "imported"
                    from .fabric.environment import overlay_weaver, read_definition

                    definition = overlay_weaver(
                        read_definition(environment_item, client=rest),
                        dev=False,
                        source=request.environment,
                    )
                    files.update(
                        {
                            f"{directory}/{path}": content
                            for path, content in definition.parts.items()
                        }
                    )
                elif (destination / directory).is_dir():
                    # Preserve adopted packages and binary custom libraries on a rerun.
                    for path in (destination / directory).rglob("*"):
                        if path.is_file():
                            files[path.relative_to(destination).as_posix()] = (
                                path.read_bytes()
                            )
            with opened.step("Checking the project files", str(destination)):
                _refuse_overwrites(destination, files)
                _check_destination_paths(destination, files)
                _parse_generated(files, request, destination=destination)

            if dry_run:
                return InitialiseReport(
                    repository=str(destination),
                    workspace=request.workspace,
                    resources=_planned(request, found),
                    files=tuple(sorted(files)),
                    example=ExampleOutcome(generated=request.example),
                    dry_run=True,
                    environment_definition=definition_status,
                )

            resources.extend(
                _create_missing(
                    request, found, physical=physical, session=opened, client=rest
                )
            )
            with opened.step("Writing the project files", str(destination)):
                _write(destination, files)
            if state == MISSING:
                from .fabric.environment import create_with_definition
                from .fabric.environment_definition import read_environment_definition

                with opened.step("Creating the Environment", request.environment):
                    try:
                        create_with_definition(
                            physical,
                            request.environment,
                            read_environment_definition(destination / directory),
                            client=rest,
                        )
                    except WeaverError as exc:
                        raise _creation_error(
                            ENVIRONMENT_ROLE, request.environment, exc
                        ) from exc
            resources.append(
                FabricItemOutcome(
                    ENVIRONMENT_ROLE,
                    request.environment,
                    CREATED if state == MISSING else EXISTING,
                )
            )
            publication = "deferred"
            if publish_environment:
                from .fabric import publish_environment as publish

                with opened.step("Publishing the Environment", request.environment):
                    result = publish(
                        request.workspace,
                        path=destination / directory,
                        session=opened,
                        client=rest,
                    )
                publication = (
                    "already published" if result.action == UNCHANGED else "published"
                )
            outcome = ExampleOutcome(generated=request.example)

    return InitialiseReport(
        repository=str(destination),
        workspace=request.workspace,
        resources=_in_role_order(resources),
        files=tuple(sorted(files)),
        example=outcome,
        environment_publication=publication,
        environment_definition=definition_status,
        dry_run=False,
    )


def _bare(request: ProjectRequest):
    """The workspace this run addresses, before the project describes it.

    Enough to open a Session on and to reach Fabric's control plane through.
    What the project declares is read back from the file it writes.
    """

    from .workspaces import Workspace

    return Workspace(workspace=request.workspace)


def _workspace_name(workspace: str | None, *, session) -> str:
    """The Fabric workspace this run addresses, named or discovered."""

    if workspace is not None:
        return str(workspace)
    inherited = getattr(session, "workspace", None)
    if inherited is not None:
        return inherited.workspace
    from .sessions.host import current_workspace_name

    discovered = current_workspace_name()
    if discovered:
        return discovered
    raise InitialiseError(
        "A Fabric workspace could not be found.\n"
        "\n"
        "If you're running from your desktop, provide the workspace name:\n"
        "\n"
        '  weaver initialise my-project --workspace "My Fabric Workspace"\n'
        "\n"
        "If you're running inside a Fabric notebook, the current workspace\n"
        "will be used automatically."
    )


# --- the files -----------------------------------------------------------------


def _generated_files(request: ProjectRequest) -> dict[str, str]:
    """Every file this request writes, as project-relative path to text."""

    files = dict(project_files(request))
    files.update(environment_definition_files(request.environment))
    if request.example:
        files.update(example_files(request))
    return files


def _parse_generated(files, request: ProjectRequest, *, destination=None):
    """Validate the destination overlaid with generated files in a temporary copy."""

    from .config import load_workspace
    from .fabric.environment_definition import read_environment_definition
    from .operations.check import check

    directory = environment_directory(request.environment)
    with tempfile.TemporaryDirectory(prefix="weaver-initialise-") as temporary:
        root = Path(temporary)
        if destination is not None and destination.exists():
            import shutil

            if not destination.is_dir():
                raise InitialiseError(f"{destination} is not a directory.")
            if (destination / "pyproject.toml").exists() and not (
                destination / WORKSPACE_CONFIG_FILE
            ).exists():
                raise InitialiseError(
                    "This folder contains files that conflict with a Weaver project. Choose a new project folder."
                )
            shutil.copytree(
                destination,
                root,
                dirs_exist_ok=True,
                ignore=shutil.ignore_patterns(".git", ".venv", "__pycache__"),
            )
        _write(root, files)
        try:
            configured = load_workspace(root / WORKSPACE_CONFIG_FILE)
            if (root / directory).is_dir():
                read_environment_definition(root / directory)
            check(root)
        except WeaverError as exc:
            raise InitialiseError(
                f"The generated project did not parse: {exc}"
            ) from exc
    return configured


def _refuse_overwrites(destination: Path, files: dict[str, str]) -> None:
    """Refuse a destination where setup would replace existing content."""

    edited = sorted(
        relative
        for relative, text in files.items()
        if (destination / relative).is_file()
        and (destination / relative).read_bytes()
        != (text.encode("utf-8") if isinstance(text, str) else text)
    )
    if not edited:
        return
    listed = "\n".join(f"  {relative}" for relative in edited)
    raise InitialiseError(
        f"Initialise would overwrite files in {destination}:\n{listed}\n"
        "Choose another project folder."
    )


def _write(destination: Path, files: dict[str, str]) -> None:
    """Write every generated file, creating the directories they sit in."""

    for relative, text in sorted(files.items()):
        path = destination / relative
        if path.resolve() != destination.resolve() / relative or path.is_symlink():
            raise InitialiseError(f"Generated path escapes the project: {relative}")
        path.parent.mkdir(parents=True, exist_ok=True)
        content = text.encode("utf-8") if isinstance(text, str) else text
        if not path.is_file() or path.read_bytes() != content:
            path.write_bytes(content)


# --- the Fabric Environment ----------------------------------------------------


#: No Environment of that name in the workspace.
MISSING = "missing"


def available_environments(workspace: str, *, client=None) -> tuple[str, ...]:
    """Every Fabric Environment in a workspace, by name, for a caller to offer."""

    from .fabric.resources import ENVIRONMENT, find_workspace, list_items

    physical = find_workspace(workspace, client=client)
    return tuple(
        sorted(
            item.name
            for item in list_items(physical, item_type=ENVIRONMENT, client=client)
        )
    )


# --- the items -----------------------------------------------------------------


@dataclass(frozen=True)
class _Requested:
    """One requested item, as the role it fills and the Fabric type it must be."""

    role: str
    name: str
    item_type: str


def _requested(request: ProjectRequest) -> tuple[_Requested, ...]:
    """The project's requested catalogue, targets and Environment."""

    from .fabric.resources import ENVIRONMENT
    from .fabric.resources import LAKEHOUSE as LAKEHOUSE_ITEM
    from .fabric.resources import WAREHOUSE as WAREHOUSE_ITEM

    wanted = [_Requested(CATALOGUE_ROLE, request.catalogue, WAREHOUSE_ITEM)]
    if request.lakehouse:
        wanted.append(_Requested(LAKEHOUSE, request.lakehouse, LAKEHOUSE_ITEM))
    if request.warehouse:
        wanted.append(_Requested(WAREHOUSE, request.warehouse, WAREHOUSE_ITEM))
    wanted.append(_Requested(ENVIRONMENT_ROLE, request.environment, ENVIRONMENT))
    return tuple(wanted)


def _read_the_workspace(request: ProjectRequest, *, client):
    """Read requested item identities from one workspace listing."""

    from .fabric.resources import (
        FACET_TYPES,
        find_workspace,
        list_items,
    )

    physical = find_workspace(request.workspace, client=client)
    held = [
        item
        for item in list_items(physical, client=client)
        if item.type not in FACET_TYPES
    ]
    by_name: dict[str, set[str]] = {}
    for item in held:
        by_name.setdefault(item.name, set()).add(item.type)

    found: dict[str, bool] = {}
    for wanted in _requested(request):
        types = by_name.get(wanted.name, set())
        if types and wanted.item_type not in types:
            other = ", ".join(sorted(types))
            raise InitialiseError(
                f"'{wanted.name}' already exists in '{request.workspace}' as "
                f"{_article(other)} {other}.\n"
                "\n"
                f"Choose another name for the {wanted.role}, or name an item\n"
                "that is already there."
            )
        found[wanted.role] = bool(types)

    environment = next(
        (
            item
            for item in held
            if item.name == request.environment and item.type == "Environment"
        ),
        None,
    )
    return EXISTING if environment else MISSING, found, physical, environment


def _planned(
    request: ProjectRequest, found: dict[str, bool]
) -> tuple[FabricItemOutcome, ...]:
    """What a run would do to each requested item, having changed nothing.

    Reported in the order a real run reports them, so what a dry run shows and
    what the run itself shows are the same list.
    """

    return _in_role_order(
        FabricItemOutcome(
            role=wanted.role,
            name=wanted.name,
            status=EXISTING if found[wanted.role] else PLANNED,
        )
        for wanted in _requested(request)
    )


def _create_missing(
    request: ProjectRequest, found: dict[str, bool], *, physical, session, client
) -> tuple[FabricItemOutcome, ...]:
    """Create missing catalogue and target items, and reuse existing ones."""

    from .fabric.resources import LAKEHOUSE as LAKEHOUSE_ITEM
    from .fabric.resources import create_lakehouse, create_warehouse

    made = []
    for wanted in _requested(request):
        if wanted.role == ENVIRONMENT_ROLE:
            continue
        if found[wanted.role]:
            made.append(FabricItemOutcome(wanted.role, wanted.name, EXISTING))
            continue
        create = (
            create_lakehouse if wanted.item_type == LAKEHOUSE_ITEM else create_warehouse
        )
        try:
            with session.step(f"Creating the {wanted.role}", wanted.name):
                create(physical, wanted.name, client=client)
        except WeaverError as exc:
            raise _creation_error(wanted.role, wanted.name, exc) from exc
        made.append(FabricItemOutcome(wanted.role, wanted.name, CREATED))
    return tuple(made)


#: The order the resources are reported in, which is the order they are set up.
_ROLE_ORDER = (CATALOGUE_ROLE, ENVIRONMENT_ROLE, LAKEHOUSE, WAREHOUSE)


def _in_role_order(resources) -> tuple[FabricItemOutcome, ...]:
    return tuple(sorted(resources, key=lambda outcome: _ROLE_ORDER.index(outcome.role)))


def _article(noun: str) -> str:
    return "an" if noun[:1].upper() in "AEIOU" else "a"


def _check_destination_paths(destination, files):
    for relative in files:
        path = destination / relative
        if Path(relative).is_absolute() or ".." in Path(relative).parts:
            raise InitialiseError(f"Invalid generated path: {relative}")
        if path.resolve() != destination.resolve() / relative:
            raise InitialiseError(f"Generated path follows a symbolic link: {relative}")
        if path.is_dir() or any(parent.is_file() for parent in path.parents):
            raise InitialiseError(
                f"This folder conflicts with a generated file: {relative}"
            )


def available_items(workspace, kind, *, client=None):
    """List display names of one item type for project setup."""

    from .fabric.resources import find_workspace, list_items

    return tuple(
        sorted(
            item.name
            for item in list_items(
                find_workspace(workspace, client=client), item_type=kind, client=client
            )
            if item.type == kind
        )
    )


def _creation_error(role, name, exc):
    """Translate Fabric validation failures while retaining diagnostic logs."""

    import logging

    from .fabric.client import FabricError

    if isinstance(exc, FabricError) and exc.status_code == 400:
        logging.getLogger(__name__).debug(
            "Fabric item validation failed", exc_info=True
        )
        return InitialiseError(
            f"Fabric rejected the {role} {name!r}. Check the item name and retry."
        )
    return InitialiseError(
        f"The {role} {name!r} could not be created: {exc}. "
        "Rerun initialise after resolving the error; existing items are reused."
    )

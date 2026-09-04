"""Set up a Weaver project and the Fabric items it needs.

One operation, reached as ``weaver.initialise(...)`` and as ``weaver
initialise``. It collects names, validates what can be validated, creates or
reuses the requested Fabric items, writes the project, and can build, load and
test a small Sales example against it.

Naming an item here is the request to have it, so nothing asks per item.
``dry_run`` lists what a run would do and changes nothing. A rerun reuses
whatever already exists, which is what makes a run that stopped part-way safe to
repeat.

The Weaver catalogue's own ``_`` tables are not created here. This provisions the
Warehouse they live in, and the first ordinary build creates them.
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .build_bundle.models import BuildPlan
from .build_bundle.report import InstallationReport
from .build_bundle.targets import ItemBinding, ItemBindings, WarehouseBinding
from .build_bundle.workflow import build_item_repository_source
from .catalogue.tables import CATALOGUE_TABLES
from .declaration.model import LAKEHOUSE, WAREHOUSE, WeaverItemId
from .errors import CommandError, WeaverError
from .locations import Location
from .onboarding import (
    WORKSPACE_CONFIG_FILE,
    ProjectRequest,
    environment_definition_files,
    example_files,
    project_files,
)
from .onboarding.environment import environment_directory
from .store import FilesystemStore, Store
from .targets import ItemRef

#: The names a caller may leave to the defaults.
DEFAULT_CATALOGUE = "Catalogue"
DEFAULT_ENVIRONMENT = "Weaver"

#: What this run did to one Fabric item. The CLI turns these into display text.
CREATED = "created"
EXISTING = "existing"
PUBLISHED = "published"
UNCHANGED = "unchanged"
WRITTEN = "written"
PLANNED = "planned"

#: What each requested item is to the project.
CATALOGUE_ROLE = "Catalogue"
ENVIRONMENT_ROLE = "Environment"


class InitialiseError(WeaverError):
    """Raised when a project cannot be set up as it was asked for."""


@dataclass(frozen=True)
class FabricItemOutcome:
    """One requested Fabric item, and what this run did about it."""

    role: str
    name: str
    status: str

    def to_mapping(self) -> dict[str, str]:
        return {"role": self.role, "name": self.name, "status": self.status}


@dataclass(frozen=True)
class ExampleOutcome:
    """Whether the Sales example was written, and how running it went.

    Each stage carries the status its own report gave, and ``succeeded`` is what
    those reports answered. Build, load and test each spell success their own
    way, so the answer is taken from them and not read back off the words.
    """

    generated: bool = False
    build: str | None = None
    load: str | None = None
    test: str | None = None
    succeeded: bool = True

    @property
    def ran(self) -> bool:
        return self.build is not None

    def to_mapping(self) -> dict[str, Any]:
        return {
            "generated": self.generated,
            "build": self.build,
            "load": self.load,
            "test": self.test,
            "succeeded": self.succeeded,
        }


@dataclass(frozen=True)
class InitialiseReport:
    """What one initialise run set up, for a caller to print or assert on."""

    repository: str
    workspace: str
    resources: tuple[FabricItemOutcome, ...] = ()
    files: tuple[str, ...] = ()
    example: ExampleOutcome = field(default_factory=ExampleOutcome)
    dry_run: bool = False

    @property
    def created(self) -> tuple[str, ...]:
        """Every Fabric item this run made, in the order it made them.

        What a run that stopped part-way leaves behind, so the next one is read
        against what is already there.
        """

        return tuple(
            f"{outcome.role}/{outcome.name}"
            for outcome in self.resources
            if outcome.status == CREATED
        )

    @property
    def succeeded(self) -> bool:
        return self.example.succeeded

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
            "dry_run": self.dry_run,
            "next_commands": list(self.next_commands),
        }


# --- the operation -------------------------------------------------------------


def initialise(
    repository=None,
    *,
    workspace: str | None = None,
    catalogue: str = DEFAULT_CATALOGUE,
    environment: str = DEFAULT_ENVIRONMENT,
    lakehouse: str | None = None,
    warehouse: str | None = None,
    example: bool = False,
    publish_environment: bool | None = None,
    dry_run: bool = False,
    session=None,
    client=None,
) -> InitialiseReport:
    """Set up a Weaver project and the Fabric items it needs.

    ``repository`` is where the project is written, defaulting to the current
    directory. In a Fabric notebook that is usually ``Path("builtin") /
    "repository"``.

    ``workspace`` names an existing Fabric workspace. Omitted inside a Fabric
    notebook, the notebook's own workspace is used. ``catalogue``,
    ``environment``, ``lakehouse`` and ``warehouse`` are plain item names, and
    each one that does not exist yet is created.

    ``example`` writes a small Sales example and runs build, load and test
    against it. ``publish_environment`` decides whether the Environment is
    published as well as written, and defaults to publishing from a desktop and
    to writing alone inside a Fabric session, where Weaver is already installed.
    ``dry_run`` reports what a run would do and changes nothing.
    """

    destination = Path.cwd() if repository is None else Path(repository)
    request = ProjectRequest(
        workspace=_workspace_name(workspace, session=session),
        catalogue=catalogue,
        environment=environment,
        lakehouse=lakehouse,
        warehouse=warehouse,
        example=example,
    )
    files = _generated_files(request)
    configured = _parse_generated(files, request)
    _refuse_edited_files(destination, files)

    from .fabric.resources import find_workspace

    physical = find_workspace(request.workspace, client=client)
    found = _inspect(physical, request, client=client)
    if dry_run:
        # Nothing after this point reads a session, and nothing before it
        # changed anything, so a dry run stops with the workspace read alone.
        return InitialiseReport(
            repository=str(destination),
            workspace=request.workspace,
            resources=_planned(request, found),
            files=tuple(sorted(files)),
            example=ExampleOutcome(generated=request.example),
            dry_run=True,
        )

    from .sessions.host import inside_fabric_session, use_or_create_session

    if publish_environment is None:
        # Inside the Fabric session being addressed, Weaver is already installed
        # by the notebook's own %pip install, and the Environment is what a later
        # desktop run needs. Publishing is the slowest step, so it waits.
        publish_environment = not inside_fabric_session(configured)

    resources = []
    with use_or_create_session(session, workspace=configured) as opened:
        with opened.task("Setting up your Weaver project", request.workspace):
            resources.extend(
                _create_missing(physical, request, found, session=opened, client=client)
            )
            with opened.step("Writing the project files", str(destination)):
                _write(destination, files)
            resources.append(
                _environment_outcome(
                    request,
                    destination,
                    publish=publish_environment,
                    session=opened,
                    client=client,
                )
            )
            outcome = (
                _run_example(request, destination, session=opened)
                if request.example
                else ExampleOutcome()
            )

    return InitialiseReport(
        repository=str(destination),
        workspace=request.workspace,
        resources=_in_role_order(resources),
        files=tuple(sorted(files)),
        example=outcome,
        dry_run=False,
    )


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
        '  weaver initialise --workspace "My Fabric Workspace"\n'
        "\n"
        "If you're running inside a Fabric notebook, the current workspace\n"
        "will be used automatically."
    )


def _generated_files(request: ProjectRequest) -> dict[str, str]:
    """Every file this request writes, as project-relative path to text."""

    files = dict(project_files(request))
    files.update(environment_definition_files(request.environment))
    if request.example:
        files.update(example_files(request))
    return files


def _parse_generated(files: dict[str, str], request: ProjectRequest):
    """Read the generated project back with the parsers a user's project uses.

    A temporary copy, so a project that would not parse is reported before any
    Fabric item is created and before anything is written where it was asked
    for. The configuration it reads is also the Workspace the run then uses, so
    the session runs against the file the project was given.
    """

    from .config import load_workspace
    from .fabric.environment_definition import read_environment_definition
    from .operations.check import check

    with tempfile.TemporaryDirectory(prefix="weaver-initialise-") as temporary:
        root = Path(temporary)
        _write(root, files)
        try:
            configured = load_workspace(root / WORKSPACE_CONFIG_FILE)
            read_environment_definition(
                root / environment_directory(request.environment)
            )
            check(root)
        except WeaverError as exc:
            raise InitialiseError(
                f"The generated project did not parse: {exc}"
            ) from exc
    return configured


def _refuse_edited_files(destination: Path, files: dict[str, str]) -> None:
    """Refuse to overwrite a generated file whose content has been changed.

    A rerun of the same request finds identical files and writes them again,
    which is what makes repeating a stopped run safe. A file the project has
    edited since is named here, and the run stops.
    """

    edited = sorted(
        relative
        for relative, text in files.items()
        if (destination / relative).is_file()
        and (destination / relative).read_text(encoding="utf-8") != text
    )
    if not edited:
        return
    listed = "\n".join(f"  {relative}" for relative in edited)
    raise InitialiseError(
        f"{destination} already holds a Weaver project, and these files have "
        "been changed since they were written:\n"
        "\n"
        f"{listed}\n"
        "\n"
        "Set the project up in an empty directory, or move these files aside\n"
        "and run it again."
    )


def _write(destination: Path, files: dict[str, str]) -> None:
    """Write every generated file, creating the directories they sit in."""

    for relative, text in sorted(files.items()):
        path = destination / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")


# --- the Fabric items ----------------------------------------------------------


@dataclass(frozen=True)
class _Requested:
    """One requested item, as the role it fills and the Fabric type it must be."""

    role: str
    name: str
    item_type: str


def _requested(request: ProjectRequest) -> tuple[_Requested, ...]:
    """Every item this project needs, in the order they are created.

    The catalogue Warehouse first, because a project without it has nowhere to
    record what it built. The Environment last, because publishing it is the
    slowest step and nothing before it depends on the result.
    """

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


def _inspect(physical, request: ProjectRequest, *, client) -> dict[str, bool]:
    """Which requested items the workspace already holds, read in one listing.

    A name held by an item of another type is refused here, before anything is
    created. A Lakehouse's generated SQL endpoint shares its display name, and
    is not a conflict.
    """

    from .fabric.resources import FACET_TYPES, list_items

    held = [
        item
        for item in list_items(physical, client=client)
        if item.type not in FACET_TYPES
    ]
    by_name: dict[str, set[str]] = {}
    for item in held:
        by_name.setdefault(item.name, set()).add(item.type)

    present: dict[str, bool] = {}
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
        present[wanted.role] = bool(types)
    return present


def _planned(
    request: ProjectRequest, found: dict[str, bool]
) -> tuple[FabricItemOutcome, ...]:
    """What a run would do to each requested item, having changed nothing."""

    return tuple(
        FabricItemOutcome(
            role=wanted.role,
            name=wanted.name,
            status=EXISTING if found[wanted.role] else PLANNED,
        )
        for wanted in _requested(request)
    )


def _create_missing(
    physical, request: ProjectRequest, found: dict[str, bool], *, session, client
) -> tuple[FabricItemOutcome, ...]:
    """Create the requested items the workspace does not hold, and reuse the rest.

    The Environment is not created here. Publishing its definition creates it,
    and that runs once the project has been written.
    """

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
            raise InitialiseError(
                f"The {wanted.role} '{wanted.name}' could not be created.\n"
                "\n"
                f"Fabric returned: {exc}\n"
                "\n"
                "Fix that and run `weaver initialise` again. Items that already\n"
                "exist are reused."
            ) from exc
        made.append(FabricItemOutcome(wanted.role, wanted.name, CREATED))
    return tuple(made)


def _environment_outcome(
    request: ProjectRequest,
    destination: Path,
    *,
    publish: bool,
    session,
    client,
) -> FabricItemOutcome:
    """Publish the generated Environment definition, or leave it written.

    Publishing sends the definition whole and creates the Environment where the
    workspace has none, so nothing here creates the item separately.
    """

    if not publish:
        return FabricItemOutcome(ENVIRONMENT_ROLE, request.environment, WRITTEN)

    from .fabric import publish_environment as publish_definition

    directory = destination / environment_directory(request.environment)
    try:
        with session.step("Publishing the Environment", request.environment):
            result = publish_definition(
                request.workspace,
                path=directory,
                session=session,
                client=client,
            )
    except WeaverError as exc:
        raise InitialiseError(
            f"The Environment '{request.environment}' could not be published.\n"
            "\n"
            f"Fabric returned: {exc}\n"
            "\n"
            "The project has been written. Fix that and run `weaver initialise`\n"
            "again, or publish it on its own with\n"
            f"`weaver fabric environment publish --path {directory}`."
        ) from exc
    return FabricItemOutcome(
        ENVIRONMENT_ROLE,
        request.environment,
        UNCHANGED if not result.published else PUBLISHED,
    )


def _run_example(
    request: ProjectRequest, destination: Path, *, session
) -> ExampleOutcome:
    """Build, load and test the generated example, stopping at the first failure."""

    from .operations.build import build
    from .operations.load import load
    from .operations.test import test

    with session.step("Creating the Sales example"):
        with session.substep("Building"):
            built = build(destination, session=session)
        if not built.succeeded:
            return ExampleOutcome(generated=True, build=built.status, succeeded=False)
        with session.substep("Loading"):
            loaded = load(session=session)
        if not loaded.succeeded:
            return ExampleOutcome(
                generated=True, build=built.status, load=loaded.status, succeeded=False
            )
        with session.substep("Testing"):
            tested = test(session=session)
    return ExampleOutcome(
        generated=True,
        build=built.status,
        load=loaded.status,
        test=tested.status,
        succeeded=tested.succeeded,
    )


#: The order the resources are reported in, which is the order they are set up.
_ROLE_ORDER = (CATALOGUE_ROLE, ENVIRONMENT_ROLE, LAKEHOUSE, WAREHOUSE)


def _in_role_order(resources) -> tuple[FabricItemOutcome, ...]:
    return tuple(sorted(resources, key=lambda outcome: _ROLE_ORDER.index(outcome.role)))


def _article(noun: str) -> str:
    return "an" if noun[:1].upper() in "AEIOU" else "a"


# --- the Warehouse the catalogue lives in --------------------------------------


@dataclass(frozen=True)
class CatalogueBuildResult:
    """What building the package-owned catalogue item did.

    Internal to this module and to the Fabric test estate that stands a
    catalogue up. A caller setting a project up reads
    :class:`InitialiseReport` instead.
    """

    item: str
    catalogue: str
    plan: BuildPlan
    report: InstallationReport

    @property
    def succeeded(self) -> bool:
        return self.report.status == "succeeded"

    @property
    def tables(self) -> tuple[str, ...]:
        """Every ``_`` table this created, however each one is maintained."""

        return tuple(table.qualified for table in CATALOGUE_TABLES)

    def to_mapping(self) -> dict[str, Any]:
        return {
            "item": self.item,
            "catalogue": self.catalogue,
            "bundle_id": self.plan.bundle_id,
            "status": self.report.status,
            "tables": list(self.tables),
        }


@dataclass(frozen=True)
class PreparedCatalogueHost:
    """The Warehouse the catalogue will live in, and whether this made it."""

    workspace: str
    catalogue: str
    created: bool


def prepare_catalogue(
    workspace,
    *,
    store: Store | None = None,
    client=None,
) -> PreparedCatalogueHost:
    """Find or create the Warehouse the Weaver catalogue lives in.

    An existing Warehouse is the ordinary case rather than a collision: Weaver
    owns the ``_`` schema of its host and nothing else, so a Warehouse already
    holding a user's schemas is a perfectly good catalogue host. What is
    distinguished here is only whether the Warehouse existed. Whether its `_`
    tables are there is the build's question, answered by reading them.
    """

    if not workspace.catalogue:
        raise CommandError("initialise requires a configured Weaver catalogue")
    name = workspace.catalogue_item.name
    from .fabric.resources import (
        WAREHOUSE,
        ItemNotFoundError,
        create_warehouse,
        find_item,
        find_workspace,
    )

    physical_workspace = find_workspace(workspace.workspace, client=client)
    try:
        find_item(physical_workspace, name, item_type=WAREHOUSE, client=client)
        created = False
    except ItemNotFoundError:
        create_warehouse(physical_workspace, name, client=client)
        created = True
    return PreparedCatalogueHost(workspace.workspace, name, created)


def _session_around(workspace, *, spark, store):
    """A Session wrapped around resources the caller already holds.

    Both are given, so the Session closes neither. This is how a caller that
    is already inside its own Spark session, such as a notebook or a test holding
    one open for a module, reaches the build path without the build acquiring a
    second one.
    """

    from .sessions import ConsoleSession

    return ConsoleSession(workspace=workspace, spark=spark, store=store)


def initialise_catalogue(
    *,
    catalogue: ItemRef,
    workspace,
    store: Store,
    spark: Any = None,
    output: Location | None = None,
    session=None,
) -> CatalogueBuildResult:
    """Build the built-in Weaver item alone, through the ordinary build path.

    A compatibility wrapper and nothing more. It owns no catalogue DDL, no
    catalogue publication and no control-plane preparation: it selects no
    authored item, and the built-in ``Warehouse/_weaver`` that every build
    composes in is therefore the whole of what it builds.

    Ordinary builds compose in and bind the same Item directly.

    An empty source directory is the input because the built-in item is composed
    into a parsed repository rather than authored into one: there is nothing
    for a caller to supply, and supplying a real repository here would silently
    ignore it.
    """

    control = WarehouseBinding(warehouse=catalogue)
    bindings = ItemBindings(
        (
            ItemBinding(
                WeaverItemId.parse("Warehouse/_weaver"),
                control,
            ),
        )
    )
    from .sessions.host import use_or_create_session

    # A Session built around what this caller already holds: the Spark it is
    # running in and the store it reads through are given, so nothing here
    # acquires or closes a resource it did not open.
    owned = (
        None
        if session is not None
        else _session_around(workspace, spark=spark, store=store)
    )
    with use_or_create_session(session or owned, workspace=workspace) as opened:
        with tempfile.TemporaryDirectory(prefix="weaver-initialise-") as temporary:
            repository_root = Path(temporary) / "repository"
            repository_root.mkdir()
            result = build_item_repository_source(
                Location(repository_root.as_posix()),
                source_store=FilesystemStore(),
                bindings=bindings,
                session=opened,
                workspace=workspace,
                catalogue_binding=control,
                output=output,
            )

    return CatalogueBuildResult(
        item="Warehouse/_weaver",
        catalogue=catalogue.name,
        plan=result.plan,
        report=result.report,
    )

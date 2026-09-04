"""Set up a Weaver project and the Fabric items it needs.

One operation, reached as ``weaver.initialise(...)`` and as ``weaver
initialise``. It collects names, validates what can be validated, creates or
reuses the requested Fabric items, writes the project, and can build, load and
test a small Sales example against it.

Naming an item here is the request to have it, so nothing asks per item.
``dry_run`` lists what a run would do and changes nothing. A rerun reuses
whatever already exists, which is what makes a run that stopped part-way safe to
repeat.

Every project runs against a Fabric Environment with Weaver installed in it.
Whether that Environment is one the workspace already has or one this run
creates, installing Weaver there takes minutes, so ``install_weaver`` is the
caller's consent to spend them. Without it, an Environment that is not ready is
a failure reported before anything changes.

The Weaver catalogue's own ``_`` tables are not created here. This provisions the
Warehouse they live in, and the first ordinary build creates them.
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
#: The Environment is there with Weaver installed in it.
READY = "ready"
#: Weaver will be installed in the Environment, which is the slow part.
INSTALL = "install"

#: What the Environment machinery did, kept beside the status: the status says
#: Weaver is installed there, and this says whether the item was made.
UPDATED = "updated"
UNCHANGED = "unchanged"

#: What each requested item is to the project.
CATALOGUE_ROLE = "Catalogue"
ENVIRONMENT_ROLE = "Environment"


class InitialiseError(WeaverError):
    """Raised when a project cannot be set up as it was asked for."""


@dataclass(frozen=True)
class FabricItemOutcome:
    """One requested Fabric item, and what this run did about it.

    ``status`` is what a person reads. ``action`` is what the Fabric machinery
    reported underneath it, for an Environment: created, updated or unchanged.
    """

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
        against what is already there. An Environment reports what it did as its
        action, because its status says whether Weaver is installed there.
        """

        return tuple(
            f"{outcome.role}/{outcome.name}"
            for outcome in self.resources
            if CREATED in (outcome.status, outcome.action)
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
    install_weaver: bool = False,
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

    ``environment`` names the Fabric Environment this project runs against. It
    may be one the workspace already has. Weaver has to be installed there, and
    installing it takes minutes, so ``install_weaver`` is the consent to do so.
    Without it, an Environment that is missing or has no Weaver in it is
    reported before anything changes.

    ``example`` writes a small Sales example and runs build, load and test
    against it. ``dry_run`` reports what a run would do and changes nothing.
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

    from .sessions.host import use_or_create_session

    addressed = _bare(request)
    resources = []
    with use_or_create_session(session, workspace=addressed) as opened:
        # Every crossing goes through the Session's own client, so what this
        # asked of Fabric is attributed and counted where the rest of Weaver's
        # is. Constructing one here would leave the control-plane reads
        # invisible to the Session that owns them. The workspace is named,
        # because a borrowed Session may be open on another one.
        rest = client if client is not None else opened.resolver(addressed).client
        state, found = _read_the_workspace(request, client=rest)

        # Ownership decides both what is written and which publication mode
        # applies, so it is read once and carried into both.
        owned = _defines_environment(destination, request, state)
        files = _generated_files(request, define_environment=owned)
        configured = _parse_generated(files, request)
        _refuse_edited_files(destination, files)

        if dry_run:
            return InitialiseReport(
                repository=str(destination),
                workspace=request.workspace,
                resources=_planned(request, found, state),
                files=tuple(sorted(files)),
                example=ExampleOutcome(generated=request.example),
                dry_run=True,
            )

        decision = _decide_environment(
            request, state, install_weaver=install_weaver, owned=owned
        )
        with opened.task("Setting up your Weaver project", request.workspace):
            resources.extend(
                _create_missing(request, found, session=opened, client=rest)
            )
            with opened.step("Writing the project files", str(destination)):
                _write(destination, files)
            resources.append(
                _prepared_environment(
                    request,
                    destination,
                    decision=decision,
                    session=opened,
                    client=rest,
                )
            )
            outcome = (
                _run_example(request, destination, configured, session=opened)
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
        '  weaver initialise --workspace "My Fabric Workspace"\n'
        "\n"
        "If you're running inside a Fabric notebook, the current workspace\n"
        "will be used automatically."
    )


# --- the files -----------------------------------------------------------------


def _defines_environment(
    destination: Path, request: ProjectRequest, state: str
) -> bool:
    """Whether this project carries the definition of its own Environment.

    Two ways to be the project's: the Environment is not in Fabric yet, so this
    run creates it from a definition it writes; or the project already holds one
    under `Environment/`, which says a previous run created it.

    The second is what keeps a rerun writing the same files. Reading it off
    Fabric alone would drop the definition from the generated set as soon as the
    first run succeeded, leaving a file in the project that initialise no longer
    recognised and `_refuse_edited_files` no longer protected.

    An Environment the workspace already had, and that this project never
    defined, stays somebody else's: no definition is generated for it.
    """

    if state == MISSING:
        return True
    return (destination / environment_directory(request.environment)).is_dir()


def _generated_files(
    request: ProjectRequest, *, define_environment: bool
) -> dict[str, str]:
    """Every file this request writes, as project-relative path to text."""

    files = dict(project_files(request))
    if define_environment:
        files.update(environment_definition_files(request.environment))
    if request.example:
        files.update(example_files(request))
    return files


def _parse_generated(files: dict[str, str], request: ProjectRequest):
    """Read the generated project back with the parsers a user's project uses.

    A temporary copy, so a project that would not parse is reported before any
    Fabric item is created and before anything is written where it was asked
    for. The configuration it reads is also the Workspace the run then uses, so
    the example builds against the file the project was given.
    """

    from .config import load_workspace
    from .fabric.environment_definition import read_environment_definition
    from .operations.check import check

    directory = environment_directory(request.environment)
    with tempfile.TemporaryDirectory(prefix="weaver-initialise-") as temporary:
        root = Path(temporary)
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


# --- the Fabric Environment ----------------------------------------------------
#
# Every Weaver project runs against a Fabric Environment, and Weaver has to be
# installed in it before what the project builds can run. Three states, and one
# action that moves the first two to the third.


#: No Environment of that name in the workspace.
MISSING = "missing"
#: The Environment is there, and Weaver is not installed in it.
UNPREPARED = "unprepared"

#: What Fabric calls a publication that finished.
_PUBLISHED = frozenset({"success", "succeeded"})


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


def environment_state(workspace: str, name: str, *, client=None) -> str:
    """Whether this Environment is there, and whether Weaver is installed in it.

    Installed means both halves: the library list names Weaver, and Fabric has
    published it. A definition naming Weaver that was never published resolves
    no imports, so a session attached to it still cannot ``import weaver``.
    """

    from .fabric.resources import (
        ENVIRONMENT,
        ItemNotFoundError,
        find_item,
        find_workspace,
    )

    physical = find_workspace(workspace, client=client)
    try:
        item = find_item(physical, name, item_type=ENVIRONMENT, client=client)
    except ItemNotFoundError:
        return MISSING
    return _state_of(item, client=client)


def _state_of(item, *, client) -> str:
    from .fabric.client import FabricClient
    from .fabric.environment import publish_state, read_definition

    reachable = client if client is not None else FabricClient()
    if not _names_weaver(read_definition(item, client=reachable)):
        return UNPREPARED
    published = publish_state(item, client=reachable).casefold()
    return READY if published in _PUBLISHED else UNPREPARED


def _names_weaver(definition) -> bool:
    """Whether a definition carries the Weaver this client is.

    A custom wheel is a development publication and is taken as it is: the
    checkout's own wheel is the Weaver installed there, and its version moves
    with the source rather than with PyPI.

    A PyPI requirement has to name this Weaver. An Environment holding
    ``weaverstack==0.9.1`` is not ready for a ``0.9.0`` client, so it is
    reported unprepared and the ordinary Environment machinery republishes it.
    A development client pins nothing, so it asks only that Weaver is named.
    """

    from .fabric.environment import is_weaver_wheel
    from .fabric.environment_definition import (
        pip_entries,
        released_requirement,
        weaver_requirement,
    )

    if any(is_weaver_wheel(name) for name in definition.custom_libraries()):
        return True
    entries = pip_entries(definition.external_libraries(), source="the Environment")
    written = weaver_requirement(entries)
    if written is None:
        return False

    wanted = released_requirement()
    if "==" not in wanted:
        return True
    return _same_requirement(written, wanted)


def _same_requirement(written: str, wanted: str) -> bool:
    """Whether an authored entry asks for exactly the requirement Weaver wants."""

    from packaging.requirements import InvalidRequirement, Requirement

    try:
        authored = Requirement(written)
    except InvalidRequirement:
        return False
    return str(authored.specifier) == str(Requirement(wanted).specifier)


#: What this run will do about the Environment. Not a status: the status says
#: what a person reads afterwards, and these say which call gets made.
REUSE = "reuse"
#: The project's own definition is authoritative, and is sent whole.
FROM_DEFINITION = "from-definition"
#: Somebody else's Environment, which keeps everything it declares. Only
#: Weaver's own libraries are added.
OVERLAY = "overlay"


def _decide_environment(
    request: ProjectRequest, state: str, *, install_weaver: bool, owned: bool
) -> str:
    """What this run will do about the Environment, decided before anything moves.

    An Environment that is ready is used as it is. One that is missing or has no
    Weaver in it needs the installation, which takes minutes, so a run that was
    not given consent stops here and says how to give it.

    Ownership decides which of the two publication modes applies, and it decides
    it for a rerun as much as for a first run. A project that created its
    Environment keeps its definition authoritative afterwards, so a run
    interrupted between creating the item and finishing the publication is
    finished from the definition rather than overlaid into a half-made item.

    .. code-block:: text

        missing                     → create from the project's definition
        unprepared, project's own   → publish the project's definition again
        unprepared, somebody else's → add Weaver's libraries and nothing else
        ready                       → reuse
    """

    if state == READY:
        return REUSE
    if not install_weaver:
        raise InitialiseError(_unprepared(request.environment, state))
    return FROM_DEFINITION if owned else OVERLAY


def _unprepared(name: str, state: str) -> str:
    """What to do about an Environment this run may not prepare itself."""

    if state == MISSING:
        return (
            f"The Fabric Environment '{name}' does not exist.\n"
            "\n"
            "Run `weaver initialise` interactively if you'd like to create it,\n"
            "or provide the name of an existing Environment."
        )
    return (
        f"The Fabric Environment '{name}' does not have Weaver installed.\n"
        "\n"
        "Run `weaver initialise` interactively to install Weaver in this "
        "Environment,\n"
        "or prepare the Environment before running this command again."
    )


def _prepared_environment(
    request: ProjectRequest,
    destination: Path,
    *,
    decision: str,
    session,
    client,
) -> FabricItemOutcome:
    """The Environment, with Weaver installed in it.

    A definition the project owns is sent whole, which creates the item where
    Fabric has none and updates it where a previous run left one half-made.
    Installing into one the workspace already had touches Weaver's own libraries
    and leaves everything else in it alone, because that Environment belongs to
    whoever made it.
    """

    name = request.environment
    if decision == REUSE:
        return FabricItemOutcome(ENVIRONMENT_ROLE, name, READY, action=UNCHANGED)

    from .fabric import publish_environment

    from_definition = decision == FROM_DEFINITION
    directory = destination / environment_directory(name)
    try:
        with session.step("Installing Weaver in Fabric", name):
            result = publish_environment(
                request.workspace,
                None if from_definition else name,
                path=directory if from_definition else None,
                session=session,
                client=client,
            )
    except WeaverError as exc:
        raise InitialiseError(
            f"Weaver could not be installed in the Fabric Environment "
            f"'{name}'.\n"
            "\n"
            f"Fabric returned: {exc}\n"
            "\n"
            "The project has been written. Fix that and run `weaver initialise`\n"
            "again."
        ) from exc
    return FabricItemOutcome(ENVIRONMENT_ROLE, name, READY, action=result.action)


# --- the items -----------------------------------------------------------------


@dataclass(frozen=True)
class _Requested:
    """One requested item, as the role it fills and the Fabric type it must be."""

    role: str
    name: str
    item_type: str


def _requested(request: ProjectRequest) -> tuple[_Requested, ...]:
    """Every item this project needs, in the order they are set up.

    The catalogue Warehouse first, because a project without it has nowhere to
    record what it built. The Environment last, because installing Weaver in it
    is the slowest step and nothing before it depends on the result.
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


def _read_the_workspace(request: ProjectRequest, *, client):
    """What the workspace holds, and what state its Environment is in.

    One listing answers every item's existence, and the Environment needs two
    further reads to say whether Weaver is installed in it. Both happen before
    anything is decided, so a run that cannot proceed says so having changed
    nothing.
    """

    from .fabric.resources import (
        ENVIRONMENT,
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

    if not found[ENVIRONMENT_ROLE]:
        return MISSING, found
    environment = next(
        item
        for item in held
        if item.name == request.environment and item.type == ENVIRONMENT
    )
    return _state_of(environment, client=client), found


def _planned(
    request: ProjectRequest, found: dict[str, bool], state: str
) -> tuple[FabricItemOutcome, ...]:
    """What a run would do to each requested item, having changed nothing.

    Reported in the order a real run reports them, so what a dry run shows and
    what the run itself shows are the same list.
    """

    return _in_role_order(
        FabricItemOutcome(
            role=wanted.role,
            name=wanted.name,
            status=_planned_status(wanted, found, state),
        )
        for wanted in _requested(request)
    )


def _planned_status(wanted: _Requested, found: dict[str, bool], state: str) -> str:
    if wanted.role != ENVIRONMENT_ROLE:
        return EXISTING if found[wanted.role] else PLANNED
    if state == READY:
        return READY
    return PLANNED if state == MISSING else INSTALL


def _create_missing(
    request: ProjectRequest, found: dict[str, bool], *, session, client
) -> tuple[FabricItemOutcome, ...]:
    """Create the requested items the workspace does not hold, and reuse the rest.

    The Environment is not created here. Installing Weaver creates it where the
    workspace has none, and that runs once the project has been written.
    """

    from .fabric.resources import LAKEHOUSE as LAKEHOUSE_ITEM
    from .fabric.resources import create_lakehouse, create_warehouse, find_workspace

    physical = find_workspace(request.workspace, client=client)
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


def _run_example(
    request: ProjectRequest, destination: Path, configured, *, session
) -> ExampleOutcome:
    """Build, load and test the generated example, stopping at the first failure.

    Each operation names the configuration this run just wrote. A borrowed
    Session carries a workspace of its own, and its items are not the ones this
    project declares, so leaving the configuration unnamed would build the
    caller's estate instead of the example.
    """

    from .operations.build import build
    from .operations.load import load
    from .operations.test import test

    project = destination / WORKSPACE_CONFIG_FILE
    with session.step("Creating the Sales example"):
        with session.substep("Building"):
            built = build(destination, workspace_config=project, session=session)
        if not built.succeeded:
            return ExampleOutcome(generated=True, build=built.status, succeeded=False)
        with session.substep("Loading"):
            loaded = load(workspace_config=project, session=session)
        if not loaded.succeeded:
            return ExampleOutcome(
                generated=True, build=built.status, load=loaded.status, succeeded=False
            )
        with session.substep("Testing"):
            tested = test(workspace_config=project, session=session)
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

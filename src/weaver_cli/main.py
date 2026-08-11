"""Top-level CLI routing.

Commands are registered here as the core APIs they wrap become available. The
handler contract is the one convention worth keeping from the old repository:
a command function returns a plain serialisable structure and the CLI prints
it.
"""

from __future__ import annotations

import argparse
import sys
import time

import weaver
from weaver.errors import WeaverError

#: The capacity verbs, kept here so the parser needs no Fabric import — a
#: CLI-only install without the [fabric] extra must still build its parser.
CAPACITY_ACTIONS = ("status", "resume", "suspend")

#: Named in help text. Spelled out here rather than imported at module scope so
#: building the parser stays free of everything ``compose`` pulls in.
COMPOSE_DEFAULT_FILE = "compose.yml"


# --- what each command will want ----------------------------------------------
#
# Declared here, from parsed arguments alone, and deliberately coarse. A Session
# cannot work these out — it has no idea what a build is, and a Session that did
# would be a second place deciding what an operation does. So commands declare
# and the Session prepares.
#
# These are a *superset*, because arguments cannot know what a repository or a
# catalogue turns out to contain. `load Lakehouse/Sales` says Livy may be needed
# because a Lakehouse usually holds Python primitives, not because this estate
# does. Exact routing comes later, from the BuildBundle or the RunGraph, and
# nothing below treats a declaration as permission to acquire.


def _target_requirements(targets) -> set[str]:
    """What the named physical targets imply, by their type alone."""

    from weaver.session.requirements import LIVY, ONELAKE, TDS

    wanted: set[str] = set()
    for value in targets or ():
        kind = str(value).split("/", 1)[0].strip().lower()
        if kind.startswith("warehouse"):
            wanted.add(TDS)
        else:
            # A Lakehouse is files and Spark until something says otherwise.
            wanted |= {ONELAKE, LIVY}
    return wanted


def _requires_targets(args) -> frozenset[str]:
    from weaver.session.requirements import AUTH, RESOLVER, requirements

    return requirements(
        AUTH, RESOLVER, *_target_requirements(getattr(args, "targets", ()))
    )


def _requires_build(args) -> frozenset[str]:
    """A build may touch everything: it writes files, DDL and the catalogue."""

    from weaver.session.requirements import (
        AUTH,
        LIVY,
        ONELAKE,
        RESOLVER,
        TDS,
        requirements,
    )

    return requirements(AUTH, RESOLVER, ONELAKE, LIVY, TDS)


def _requires_control(args) -> frozenset[str]:
    """The catalogue lives in Delta in the Weaver Lakehouse, so Spark reaches it."""

    from weaver.session.requirements import AUTH, LIVY, RESOLVER, requirements

    return requirements(AUTH, RESOLVER, LIVY)


def _requires_rest(args) -> frozenset[str]:
    """Fabric control-plane work: a credential and the resolver, nothing more."""

    from weaver.session.requirements import AUTH, RESOLVER, requirements

    return requirements(AUTH, RESOLVER)


def command_requirements(parsed) -> frozenset[str]:
    """What one parsed command says it will want. Empty when it says nothing."""

    declares = getattr(parsed, "requires", None)
    return frozenset(declares(parsed)) if declares is not None else frozenset()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="weaver",
        description="Weaver — build and load Fabric Lakehouse and Warehouse objects.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"weaverstack {weaver.__version__}",
    )
    subcommands = parser.add_subparsers(dest="command", metavar="command")

    doctor = subcommands.add_parser(
        "doctor", help="check whether this machine can run local Spark and Delta"
    )
    doctor.add_argument("--json", action="store_true", help="emit the report as JSON")
    doctor.set_defaults(handler=handle_doctor)

    shell = subcommands.add_parser(
        "session",
        help="one persistent console session running many commands",
    )
    _add_workspace_args(shell)
    shell.add_argument(
        "--timings",
        action="store_true",
        help="on exit, report what this session spent per transport",
    )
    shell.set_defaults(handler=handle_session)

    compose = subcommands.add_parser(
        "compose",
        help="run a named sequence of Weaver commands in one session",
    )
    compose.add_argument("name", help="the composition to run, from compose.yml")
    compose.add_argument(
        "--file",
        metavar="PATH",
        help=f"composition file; defaults to ./{COMPOSE_DEFAULT_FILE}",
    )
    compose.add_argument(
        "--timings",
        action="store_true",
        help="after the sequence, report what it spent per transport",
    )
    _add_workspace_args(compose)
    compose.set_defaults(handler=handle_compose)

    build = subcommands.add_parser(
        "build", help="build bound logical items from an explicit repository"
    )
    build.add_argument(
        "repository",
        nargs="?",
        help="authored repository folder; defaults to the current directory/Notebook Resources",
    )
    build.add_argument(
        "--bind",
        dest="item_bindings",
        action="append",
        metavar="PHYSICAL[=LOGICAL]",
        help="typed physical target with configured default or logical override",
    )
    build.add_argument(
        "--bundle",
        nargs="?",
        const="",
        metavar="NAME",
        help=(
            "optionally retain a .weaver.zip build record; omit NAME for a UTC "
            "timestamp"
        ),
    )
    build.add_argument("--json", action="store_true", help="emit the result as JSON")
    _add_workspace_args(build)
    build.set_defaults(handler=handle_build, requires=_requires_build)

    push = subcommands.add_parser(
        "push", help="compatibility utility: validate and upload an authored repository"
    )
    push.add_argument("repository", help="local authored repository folder")
    push.add_argument("--json", action="store_true", help="emit the result as JSON")
    _add_workspace_args(push)
    push.set_defaults(handler=handle_push, requires=_requires_rest)

    load = subcommands.add_parser(
        "load", help="load every installed object in named physical targets"
    )
    load.add_argument(
        "targets",
        nargs="+",
        metavar="TARGET",
        help="Lakehouse/Name or Warehouse/Name",
    )
    load.add_argument(
        "--fault-tolerant",
        action="store_true",
        help="continue independent branches after a node fails, and report",
    )
    load.add_argument(
        "--dry-run",
        action="store_true",
        help="resolve and render the plan without dispatching anything",
    )
    load.add_argument("--json", action="store_true", help="emit the report as JSON")
    _add_workspace_args(load)
    load.set_defaults(handler=handle_load, requires=_requires_targets)

    validate = subcommands.add_parser(
        "test", help="run the installed Tests and Assumptions in named targets"
    )
    validate.add_argument(
        "targets",
        nargs="+",
        metavar="TARGET",
        help="Lakehouse/Name or Warehouse/Name",
    )
    selection = validate.add_mutually_exclusive_group()
    selection.add_argument(
        "--name",
        metavar="Schema.Object",
        help="run one installed validation and return its diagnostic rows",
    )
    selection.add_argument(
        "--file",
        metavar="PATH",
        help="compile and run a source file without installing it",
    )
    validate.add_argument(
        "--dry-run",
        action="store_true",
        help="report what would run without dispatching anything",
    )
    validate.add_argument("--json", action="store_true", help="emit the report as JSON")
    _add_workspace_args(validate)
    validate.set_defaults(handler=handle_test, requires=_requires_targets)

    unbind = subcommands.add_parser(
        "unbind", help="remove catalogue state for named physical targets"
    )
    unbind.add_argument(
        "targets",
        nargs="+",
        metavar="TARGET",
        help="Lakehouse/Name or Warehouse/Name",
    )
    unbind.add_argument("--json", action="store_true", help="emit the result as JSON")
    _add_workspace_args(unbind)
    unbind.set_defaults(handler=handle_unbind, requires=_requires_control)

    wipe = subcommands.add_parser(
        "wipe", help="clear a physical Lakehouse or Warehouse"
    )
    wipe.add_argument(
        "targets",
        nargs="+",
        metavar="TARGET",
        help="Lakehouse/Name[/Files|/Tables] or Warehouse/Name",
    )
    _add_workspace_args(wipe)
    wipe.add_argument(
        "--unbind-from",
        metavar="LAKEHOUSE",
        help="immediately remove wiped target claims from this Weaver catalogue",
    )
    wipe.add_argument("--dry-run", action="store_true", help="report without removing")
    wipe.add_argument("--yes", action="store_true", help="do not ask for confirmation")
    wipe.add_argument("--json", action="store_true", help="emit the result as JSON")
    wipe.set_defaults(handler=handle_wipe, requires=_requires_build)

    install = subcommands.add_parser(
        "install",
        help="build Weaver and install it into a Fabric Environment",
    )
    _add_workspace_args(install, include_weaver_lakehouse=False)
    install.add_argument(
        "--no-publish",
        action="store_true",
        help="stage the wheel and dependencies but do not publish (development only)",
    )
    install.add_argument("--json", action="store_true", help="emit the result as JSON")
    install.set_defaults(handler=handle_install, requires=_requires_rest)

    notebook = subcommands.add_parser(
        "notebook", help="deploy or execute a Fabric notebook"
    )
    notebook_commands = notebook.add_subparsers(
        dest="notebook_command", metavar="command"
    )

    notebook_push = notebook_commands.add_parser(
        "push", help="create or update a notebook definition"
    )
    notebook_push.add_argument("source", help="local .py or .ipynb notebook source")
    notebook_push.add_argument("--name", help="Fabric display name; defaults to filename")
    notebook_push.add_argument("--description")
    notebook_push.add_argument("--json", action="store_true")
    _add_workspace_args(notebook_push, include_weaver_lakehouse=False)
    notebook_push.set_defaults(handler=handle_notebook_push)

    notebook_run = notebook_commands.add_parser(
        "run", help="execute a deployed notebook in Fabric"
    )
    notebook_run.add_argument("name", help="Fabric Notebook display name")
    notebook_run.add_argument(
        "--lakehouse",
        help="default Lakehouse attached to the notebook session",
    )
    notebook_run.add_argument("--no-wait", action="store_true")
    notebook_run.add_argument("--timeout", type=float, default=7200.0)
    notebook_run.add_argument("--poll-interval", type=float, default=10.0)
    notebook_run.add_argument("--json", action="store_true")
    _add_workspace_args(notebook_run)
    notebook_run.set_defaults(handler=handle_notebook_run)

    capacity = subcommands.add_parser(
        "capacity", help="turn a Fabric capacity on or off, or report its state"
    )
    capacity.add_argument("action", choices=CAPACITY_ACTIONS)
    capacity.add_argument("--resource-group", required=True)
    capacity.add_argument("--capacity-name", required=True)
    capacity.add_argument(
        "--subscription-id",
        help="only needed when az has more than one subscription",
    )
    capacity.set_defaults(handler=handle_capacity)

    return parser


def _fabric_cli_workspace(args: argparse.Namespace):
    """Resolve the Fabric-only values shared by notebook CLI utilities."""

    from weaver.errors import CommandError
    from weaver.workspaces import FabricWorkspace

    workspace = _resolve_workspace(args)
    if not isinstance(workspace, FabricWorkspace):
        raise CommandError("weaver notebook requires a Fabric Workspace")
    return workspace


def handle_notebook_push(args: argparse.Namespace) -> int:
    """Deploy a notebook definition without adding notebook APIs to core."""

    import json

    from weaver.fabric.notebooks import push_notebook

    workspace = _fabric_cli_workspace(args)
    result = push_notebook(
        args.source,
        workspace=workspace.workspace,
        name=args.name,
        description=args.description,
    )
    if args.json:
        print(json.dumps(result.to_mapping(), indent=2))
    else:
        print(f"{result.action} notebook {result.notebook!r} in {result.workspace!r}")
        print(f"  id:     {result.notebook_id}")
        print(f"  source: {result.source}")
    return 0


def handle_notebook_run(args: argparse.Namespace) -> int:
    """Run a notebook with explicit session attachments."""

    import json

    from weaver.errors import CommandError
    from weaver.fabric.notebooks import run_notebook

    workspace = _fabric_cli_workspace(args)
    lakehouse = args.lakehouse or workspace.weaver_lakehouse
    if not lakehouse:
        raise CommandError(
            "notebook run requires --lakehouse or a configured Weaver Lakehouse"
        )
    if not workspace.environment:
        raise CommandError(
            "notebook run requires --environment or a configured Environment"
        )
    result = run_notebook(
        args.name,
        workspace=workspace.workspace,
        lakehouse=lakehouse,
        environment=workspace.environment,
        wait=not args.no_wait,
        timeout=args.timeout,
        poll_interval=args.poll_interval,
    )
    if args.json:
        print(json.dumps(result.to_mapping(), indent=2))
    else:
        print(f"notebook {result.notebook!r}: {result.status}")
        print(f"  job: {result.job_url}")
        if result.exit_value is not None:
            print(f"  result: {result.exit_value}")
    return 0 if result.succeeded or args.no_wait else 1


def handle_install(args: argparse.Namespace) -> int:
    """Build Weaver from this checkout and install it into a Fabric Environment.

    The authoritative deployment path. Afterwards a notebook, Livy session or
    Fabric pytest run attached to the Environment can ``import weaver`` with no
    source shipped into a Lakehouse.
    """

    from weaver.workspaces import FabricWorkspace
    from weaver.errors import CommandError

    workspace = _resolve_workspace(args)
    if not isinstance(workspace, FabricWorkspace):
        raise CommandError("weaver install requires a Fabric Workspace")
    if not workspace.environment:
        raise CommandError("weaver install requires --environment or a configured environment")
    _prefer_desktop_credential()
    from weaver.fabric import install as run_install

    started = time.perf_counter()
    result = run_install(
        workspace.workspace,
        workspace.environment,
        publish=not args.no_publish,
    )
    total = time.perf_counter() - started

    if args.json:
        import json

        payload = result.as_dict()
        payload["timings"]["total"] = round(total, 2)
        print(json.dumps(payload, indent=2))
        return 0

    _print_install(result, total)
    return 0


def _print_install(result, total: float) -> None:
    print("Installed Weaver into Microsoft Fabric\n")
    print("Workspace")
    print(f"  Name: {result.workspace_name}")
    print(f"  ID:   {result.workspace_id}\n")
    print("Environment")
    print(f"  Name: {result.environment_name}")
    print(f"  ID:   {result.environment_id}\n")
    print("Package")
    print(f"  Distribution: {result.package_name}")
    print(f"  Version:      {result.package_version}")
    print(f"  Wheel:        {result.wheel_filename}\n")
    print("Changes")
    print(f"  Environment created:  {'yes' if result.created_environment else 'no'}")
    print(f"  Dependencies changed: {'yes' if result.dependencies_changed else 'no'}")
    print(f"  Wheel changed:        {'yes' if result.wheel_changed else 'no'}")
    print(f"  Published:            {result.publish_status}\n")
    parts = ", ".join(f"{name} {secs:.1f}s" for name, secs in result.timings.items())
    print(f"Timing  {parts + ', ' if parts else ''}total {total:.1f}s\n")
    print("Notebook use")
    print(f'  1. Attach the "{result.environment_name}" Environment.')
    print("  2. Start a new session.")
    print("  3. Run: import weaver")


def handle_capacity(args: argparse.Namespace) -> int:
    """Report or change a capacity's state.

    Capacity is billed while it runs, so this is the first and last thing a
    Fabric session touches.
    """

    _prefer_desktop_credential()
    from weaver.fabric import run_capacity_action

    result = run_capacity_action(
        args.action,
        resource_group=args.resource_group,
        capacity_name=args.capacity_name,
        subscription_id=args.subscription_id,
    )
    print(result)
    if args.action == "resume" and not result.running:
        print("  (resuming takes a moment; run `capacity status` to confirm)")
    return 0


def _add_workspace_args(
    parser: argparse.ArgumentParser, *, include_weaver_lakehouse: bool = True
) -> None:
    """Add the explicit values that a Workspace configuration can abbreviate."""

    parser.add_argument("--workspace", help="Fabric Workspace name or local folder path")
    parser.add_argument("--workspace-config", help="one Workspace configuration file")
    parser.add_argument(
        "--workspace-type",
        choices=("fabric", "local"),
        help="resource environment; defaults to fabric",
    )
    parser.add_argument("--environment", help="Fabric Environment name")
    if include_weaver_lakehouse:
        parser.add_argument("--weaver-lakehouse", help="control Lakehouse name")


def _prefer_desktop_credential() -> None:
    """Pin the Azure CLI credential for desktop commands.

    Credential choice is the CLI's policy, not the core's. Best-effort — if the
    Fabric extra is not installed there is nothing to pin, and a local command
    never needs it.
    """

    try:
        from weaver.fabric.auth import prefer_cli_credential
    except ImportError:
        return
    prefer_cli_credential()


def _desktop_store(workspace):
    """The store a desktop command uses to reach a workspace.

    Local is within-workspace; Fabric is cross-boundary, so the CLI constructs the
    OneLakeDfsClient here — core never turns a FabricWorkspace into a DFS client.
    """

    from weaver.workspaces import LocalWorkspace
    from weaver.store import FilesystemStore

    if isinstance(workspace, LocalWorkspace):
        return FilesystemStore()
    from weaver.fabric import OneLakeDfsClient

    return OneLakeDfsClient()


def _resolve_workspace(args: argparse.Namespace):
    """The workspace this command line means.

    Inside ``weaver session`` a command that names no workspace inherits the one
    the session was started with, and flags it *does* give are applied on top —
    so ``build --weaver-lakehouse Other`` overrides the control Lakehouse
    without having to restate the workspace. A command naming its own
    ``--workspace`` addresses that one instead, in its own scope.

    Inheritance is only ever from the session's *starting* workspace. A default
    accumulated from whichever command ran last would mean the next command
    silently borrowed another workspace's Environment.
    """

    from weaver.config import resolve_workspace
    from weaver.workspaces import LocalWorkspace

    inherited = getattr(getattr(args, "session", None), "workspace", None)
    if inherited is not None and args.workspace is None and args.workspace_config is None:
        workspace = _with_command_overrides(inherited, args)
    else:
        workspace = resolve_workspace(
            workspace=args.workspace,
            workspace_type=args.workspace_type,
            environment=args.environment,
            weaver_lakehouse=getattr(args, "weaver_lakehouse", None),
            workspace_config=args.workspace_config,
        )

    if not isinstance(workspace, LocalWorkspace):
        _prefer_desktop_credential()
    return workspace


def _with_command_overrides(workspace, args: argparse.Namespace):
    """The session's workspace, with whatever this command line said on top."""

    from dataclasses import replace

    from weaver.errors import CommandError
    from weaver.targets import ItemRef

    wanted_type = getattr(args, "workspace_type", None)
    if wanted_type is not None and wanted_type != workspace.workspace_type:
        raise CommandError(
            f"this session addresses a {workspace.workspace_type} workspace; "
            f"name a --workspace to use a {wanted_type} one"
        )

    overrides = {}
    if getattr(args, "environment", None) is not None:
        overrides["environment"] = args.environment
    if getattr(args, "weaver_lakehouse", None) is not None:
        overrides["weaver_lakehouse"] = ItemRef.parse(str(args.weaver_lakehouse)).name
    return replace(workspace, **overrides) if overrides else workspace


def _session(args: argparse.Namespace):
    """The Session this command runs in, where there is one.

    ``weaver session`` attaches one to every line it parses. A one-shot
    invocation has none, and each operation opens and closes its own.
    """

    return getattr(args, "session", None)


def handle_session(args: argparse.Namespace) -> int:
    """Hold one console session open and run commands in it."""

    from .shell import run_shell

    return run_shell(args)


def handle_compose(args: argparse.Namespace) -> int:
    """Show a named sequence, ask once, then run it in one Session."""

    from .compose import run_composition

    return run_composition(args)


#: What a developer is offered when a Task they can fix has failed. It names the
#: two keys and instructs nothing: the error above it has already said what went
#: wrong, and a build failure has already named the authored file to open. A
#: prompt that added "fix the file" would be telling somebody who just read
#: `Source: …` to do the obvious, and telling a load whose upstream is empty to
#: edit a file that is not the problem.
#:
#: It still says how to *leave*, because an interaction offering only "try
#: again" is a trap.
RETRY_PROMPT = "Enter to retry, Esc to exit."

ESC = "\x1b"
INTERRUPT = "\x03"
END_OF_FILE = "\x04"
ENTER = ("\r", "\n")


def _until_fixed(args: argparse.Namespace, attempt) -> int:
    """Run one Task, and offer to run it again from fresh inputs when it fails.

    **Retry is the whole Task from the beginning.** The repository is re-read,
    the physical state re-observed, the bundle or graph rebuilt. Nothing resumes
    inside a stale BuildBundle, RunGraph or half-settled plan — those describe a
    repository that has just been edited, which is precisely why the retry is
    happening. The estate may of course already hold work that succeeded; the
    fresh Task observes that and decides what is left to do.

    **The Session is not part of what gets rebuilt.** Its credential, resolver,
    item cache, Livy session and TDS connections are healthy — a SQL syntax
    error is a Task failure, not a resource failure — so the loop holds one open
    across attempts rather than paying for a cold start per fix. That is the
    whole point: the developer edits a file, presses Enter, and the next attempt
    begins immediately.

    **Non-interactive execution never prompts.** With nobody to ask, the first
    failure is the answer, and no Session is opened on retry's behalf.
    """

    if not _can_ask():
        return attempt()

    from weaver.session.host import use_or_create_session

    with use_or_create_session(
        _session(args), workspace=_resolve_workspace(args)
    ) as session:
        args.session = session
        while True:
            status = attempt()
            if not status:
                return status
            if not _retry_wanted():
                return status


def _retry_wanted() -> bool:
    """Ask, and wait for one key. Enter retries; Esc leaves.

    One keypress rather than a typed word, because the answer is binary and the
    hands are already on the keyboard having just saved a file. Stray keys are
    ignored rather than treated as either answer — a developer who tabs back to
    the terminal and hits an arrow key has not decided anything.
    """

    print(f"\n{RETRY_PROMPT} ", end="", file=sys.stderr, flush=True)
    try:
        while True:
            key = _read_key()
            if key in ENTER:
                print(file=sys.stderr)
                return True
            if key in (ESC, INTERRUPT, END_OF_FILE, ""):
                # Esc leaves; Ctrl-C and Ctrl-D are the operator declining, not
                # failures of their own.
                print(file=sys.stderr)
                return False
    except (EOFError, KeyboardInterrupt):
        print(file=sys.stderr)
        return False


def _read_key() -> str:
    """One keypress, unbuffered, without waiting for a line.

    ``input()`` cannot see Esc — it waits for Enter, which is the very key Esc
    is meant to be an alternative to. So the terminal is put into cbreak mode
    for exactly as long as it takes to read one character, and restored
    afterwards whatever happens.

    A bare Esc and the start of an arrow key are the same first byte. What tells
    them apart is whether anything follows immediately: a real escape sequence
    arrives in one burst, a person pressing Esc does not. So the rest of a
    sequence is read and *returned with it* — an arrow key comes back as
    ``"\\x1b[A"``, which is not Esc and is therefore ignored as a stray key.
    Draining it and returning the bare Esc would make every arrow key mean exit.
    """

    try:
        import termios
        import tty
    except ImportError:  # a platform without POSIX terminal control
        return sys.stdin.readline()[:1]

    descriptor = sys.stdin.fileno()
    try:
        saved = termios.tcgetattr(descriptor)
    except termios.error:  # not a terminal after all
        return sys.stdin.readline()[:1]

    import os
    import select

    try:
        tty.setcbreak(descriptor)
        # The file descriptor, not ``sys.stdin``: a text stream reads a whole
        # chunk into its own buffer, so the rest of an escape sequence would sit
        # in Python where ``select`` cannot see it, and every arrow key would
        # look exactly like a bare Esc.
        key = os.read(descriptor, 1).decode(errors="replace")
        if key == ESC:
            while select.select([descriptor], [], [], 0.05)[0]:
                key += os.read(descriptor, 1).decode(errors="replace")
        return key
    finally:
        termios.tcsetattr(descriptor, termios.TCSADRAIN, saved)


def _can_ask() -> bool:
    """Whether there is somebody at a terminal to answer."""

    return sys.stdin.isatty()


def _authorised(args: argparse.Namespace) -> bool:
    """Whether this invocation already carries the operator's go-ahead.

    Two spellings of one fact. ``--yes`` is what somebody types to skip the
    question for a single command; ``authorised`` is what ``compose`` sets
    after showing the whole sequence and being told yes — because having agreed
    to four commands, being asked again about the first of them is not a second
    safeguard, it is the first one repeated.
    """

    return bool(getattr(args, "yes", False) or getattr(args, "authorised", False))


def handle_push(args: argparse.Namespace) -> int:
    """Validate locally, then replace ``Files/weaver_items`` as one unit."""

    import json

    from weaver.locations import Location
    from weaver.push import push_item_repository
    from weaver.errors import CommandError
    from weaver.resolution import resolver_for

    workspace = _resolve_workspace(args)
    if not workspace.weaver_lakehouse:
        raise CommandError("push requires --weaver-lakehouse or a configured value")
    resolver = resolver_for(workspace)
    result = push_item_repository(
        Location(args.repository),
        resolver.weaver_items_root,
        destination_store=_desktop_store(workspace),
    )
    payload = result.to_mapping()
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(f"pushed {len(result.files)} file(s)")
        print(f"  from: {result.source}")
        print(f"  to:   {result.destination}")
        print(f"  signature: {result.repository_signature}")
    return 0


def handle_unbind(args: argparse.Namespace) -> int:
    import json

    from weaver.operations import _unbind_target_names
    from weaver.errors import CommandError

    lakehouses, warehouses = _unbind_target_names(args.targets)
    workspace = _resolve_workspace(args)
    if not workspace.weaver_lakehouse:
        raise CommandError("unbind requires a configured Weaver Lakehouse")
    result = _run_unbind(
        workspace,
        lakehouses=lakehouses,
        warehouses=warehouses,
        session=_session(args),
    )
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"unbound {len(result['logical_items'])} logical installation(s)")
        for target in result["targets"]:
            print(f"  {target}")
    return 0


def _run_unbind(workspace, *, lakehouses, warehouses, session=None) -> dict:
    """The core operation. The CLI's job here is the arguments, not the crossing."""

    from weaver.operations import unbind_catalogue_claims

    return unbind_catalogue_claims(
        workspace, lakehouses=lakehouses, warehouses=warehouses, session=session
    )


def handle_load(args: argparse.Namespace) -> int:
    return _until_fixed(args, lambda: _load_once(args))


def _load_once(args: argparse.Namespace) -> int:
    """Adapt command-line values to :func:`weaver.load`, wherever it has to run.

    The CLI owns exactly one thing the API does not: the *host boundary*. A load
    runs where the data is, so a desktop asking for a Fabric workspace has to
    reach into a session to get one — and that crossing is the CLI's, the same
    way it is for ``build`` and ``unbind``. Everything else — resolving the
    workspace, validating targets, planning, orchestrating, reporting — happens
    once, inside :func:`weaver.load`, whichever side of the boundary it runs on.
    """

    import json

    from weaver.errors import LoadError

    workspace = _resolve_workspace(args)
    try:
        report = _run_load(
            workspace,
            targets=args.targets,
            fault_tolerant=args.fault_tolerant,
            dry_run=args.dry_run,
            session=_session(args),
        )
    except LoadError as exc:
        # An intolerant failure. The report is the useful half of the answer and
        # the message is the other, so both are shown before the non-zero exit.
        if getattr(exc, "report", None) is not None:
            _print_load(exc.report)
        print(f"error: {exc}", file=sys.stderr)
        if getattr(exc, "task_log", None):
            print(f"  evidence: {exc.task_log}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(report.to_mapping(), indent=2))
    else:
        _print_load(report)
    return 0 if report.succeeded else 1


def _run_load(workspace, *, targets, fault_tolerant: bool, dry_run: bool, session=None):
    """One load, decided here and dispatched where each primitive lives.

    There is one call now. `weaver.load` reads the estate through Session
    capabilities, builds the graph locally and dispatches each node to whatever
    can run it — TDS for a Warehouse procedure, the run's remote scope for a
    deployed Python module — so the desktop, a notebook and the emulator differ
    only in what the Session answers.

    What this module still owns is the *preflight*: rejecting a mistyped target
    over one REST call, before anything expensive is acquired.
    """

    from weaver.session.host import use_or_create_session
    from weaver.workspaces import LocalWorkspace

    with use_or_create_session(session, workspace=workspace) as opened:
        if not isinstance(workspace, LocalWorkspace) and not opened.executes_here(
            workspace
        ):
            _refuse_absent_targets(workspace, targets, session=opened)
        return weaver.load(
            list(targets),
            workspace=workspace,
            fault_tolerant=fault_tolerant,
            dry_run=dry_run,
            session=opened,
        )


def _refuse_absent_targets(workspace, targets, *, session=None) -> None:
    """Check the requested items exist, over REST, before Spark is needed.

    A guard bought cheaply and spent well. Starting a Livy session costs tens of
    seconds and a capacity's only session slot; resolving a name over REST costs
    one call. So a request that can already be rejected — a mistyped
    ``Lakehouse/Rwa``, a Warehouse somebody deleted — is rejected before any of
    that is spent.

    Asked through the Session where there is one, so the answer joins the item
    cache every later command reads. Resolving these names against a resolver of
    this function's own would authenticate again and cache into an object thrown
    away one line later — paying the cost of the lookup and keeping none of it.

    Deliberately *only* that. The catalogue is not read, no graph is built, no
    upstream target is discovered and no inventory is fetched: those need the
    estate, the estate is inside Fabric, and asking about them here would be
    doing the remote run's work on the wrong side of the boundary. What is
    checked is exactly what the user typed.

    A genuine not-found is the missing-target error. Anything else — an expired
    credential, an unreachable tenant, a name that matches two items — keeps its
    own diagnosis, because "your Lakehouse is gone" is a bad answer to "your
    token expired".
    """

    from weaver.errors import CommandError
    from weaver.fabric import FabricResolver, ItemNotFoundError
    from weaver.fabric.resources import LAKEHOUSE, WAREHOUSE
    from weaver.targets import DeltaTarget, parse_physical_target, physical_item

    if session is not None:
        def resolve(item, *, item_type):
            return session.resolve_item(item, item_type=item_type, workspace=workspace)
    else:
        resolve = FabricResolver(workspace).resolve

    absent = []
    for value in targets:
        target = parse_physical_target(value, what="load target", error=CommandError)
        item_type = LAKEHOUSE if isinstance(target, DeltaTarget) else WAREHOUSE
        try:
            resolve(physical_item(target), item_type=item_type)
        except ItemNotFoundError:
            # What the user typed, not a re-spelling of it: this message exists
            # to help them see a typo, and showing them a normalised form is
            # showing them something they did not write.
            absent.append(value)
    if absent:
        raise CommandError(
            "no such item in "
            + f"{workspace.workspace!r}: "
            + ", ".join(absent)
            + " — check the name, or build into it first"
        )


def _print_load(report) -> None:
    """One renderer, for a report produced here or one that crossed Livy."""

    mode = "plan" if report.dry_run else "load"
    print(f"{mode} {report.status}: {', '.join(report.requested)}\n")
    for node in report.nodes:
        counts = ""
        if node.result is not None:
            counts = (
                f"  (read {node.result.rows_read}, "
                f"+{node.result.rows_inserted} "
                f"~{node.result.rows_updated} "
                f"-{node.result.rows_deleted} "
                f"!{node.result.rows_rejected})"
            )
        print(f"  {node.status:<24} {node.node_id}{counts}")
        for message in node.messages:
            if message.severity != "info":
                print(f"      {message.severity}: {message.message}")
    if report.task_log:
        print(f"\n  evidence: {report.task_log}")


def handle_test(args: argparse.Namespace) -> int:
    return _until_fixed(args, lambda: _test_once(args))


def _test_once(args: argparse.Namespace) -> int:
    """Adapt command-line values to :func:`weaver.test`, wherever it has to run.

    The same host boundary ``load`` crosses, and for the same reason: a
    validation reads the data, so it runs where the data is.

    A failing validation exits non-zero. That is what makes ``weaver test``
    usable in a pipeline — the report is the evidence and the exit code is the
    verdict — and it is why the API returns a report where this returns a
    status.
    """

    import json

    from weaver.errors import ValidationError

    workspace = _resolve_workspace(args)
    try:
        report = _run_test(
            workspace,
            targets=args.targets,
            name=args.name,
            file=args.file,
            dry_run=args.dry_run,
            session=_session(args),
        )
    except ValidationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(report.to_mapping(), indent=2))
    else:
        _print_test(report)
    return 0 if report.succeeded else 1


def _run_test(workspace, *, targets, name, file, dry_run: bool, session=None):
    """One validation run, decided here and dispatched where each check lives.

    The crossing is ``load``'s, for the reason it is ``load``'s: a Warehouse
    validation is a stored procedure TDS reaches from anywhere, and a Lakehouse
    one is a deployed module that belongs where the imports happen.
    """

    from weaver.session.host import use_or_create_session
    from weaver.workspaces import LocalWorkspace

    with use_or_create_session(session, workspace=workspace) as opened:
        if not isinstance(workspace, LocalWorkspace) and not opened.executes_here(
            workspace
        ):
            _refuse_absent_targets(workspace, targets, session=opened)
        return weaver.test(
            list(targets),
            workspace=workspace,
            name=name,
            file=file,
            dry_run=dry_run,
            session=opened,
        )




def _print_test(report) -> None:
    """One renderer, for a report produced here or one that crossed Livy."""

    print(f"test {report.status}\n")
    for node in report.nodes:
        result = node.result
        found = ""
        if result is not None and hasattr(result, "violation_count"):
            found = f"  ({result.violation_count} violation(s))"
        elif result is not None:
            found = (
                f"  ({result.missing_count} missing, "
                f"{result.unexpected_count} unexpected)"
            )
        print(f"  {node.status:<10} {node.kind:<11} {node.logical_id}{found}")
        for message in node.messages:
            print(f"      {message}")

    totals = report.totals()
    print(
        f"\n  {totals['passed']} passed, {totals['failed']} failed, "
        f"{totals['invalid']} could not run"
    )
    if report.task_log:
        print(f"  evidence: {report.task_log}")

    # The rows a targeted run asked for. Printed last, because the verdict is
    # what a reader wants first and the evidence is what they want next.
    for node in report.nodes:
        if not node.diagnostics:
            continue
        print(f"\n  {node.logical_id}:")
        for row in node.diagnostics:
            print(f"    {row}")


def handle_wipe(args: argparse.Namespace) -> int:
    """Preview, confirm, then invoke the same public wipe operation."""

    import json

    workspace = _resolve_workspace(args)
    planned = weaver.wipe(
        args.targets,
        workspace=workspace,
        unbind_from=args.unbind_from,
        dry_run=True,
        session=_session(args),
    )
    print(f"wipe on {workspace.workspace}\n")
    for report in planned.reports:
        print(f"  {report.target}")
        print(f"    {report.location}")
        for name in report.removed:
            print(f"      - {name}")
        if not report.removed:
            print("      (already empty)")
    total = planned.count
    print()

    if args.dry_run:
        if args.json:
            print(json.dumps(planned.to_mapping(), indent=2))
        else:
            print(f"{total} item(s) would be removed. Nothing was changed.")
        return 0

    if total and not _authorised(args):
        if not sys.stdin.isatty():
            print(
                f"Refusing to remove {total} item(s) without confirmation. "
                "Pass --yes, or --dry-run to preview.",
                file=sys.stderr,
            )
            return 1
        answer = input(f"Remove {total} item(s)? This cannot be undone [y/N] ")
        if answer.strip().lower() not in {"y", "yes"}:
            print("Cancelled.")
            return 1

    result = weaver.wipe(
        args.targets,
        workspace=workspace,
        unbind_from=args.unbind_from,
        session=_session(args),
    )
    if args.json:
        print(json.dumps(result.to_mapping(), indent=2))
    elif result.count:
        for report in result.reports:
            print(f"  {report.target}: removed {report.count}")
    else:
        print("Nothing to remove.")
    return 0


def handle_build(args: argparse.Namespace) -> int:
    return _until_fixed(args, lambda: _build_once(args))


def _build_once(args: argparse.Namespace) -> int:
    """Adapt command-line values to :func:`weaver.build`."""

    import json

    workspace = _resolve_workspace(args)
    result = weaver.build(
        args.repository,
        bind=args.item_bindings,
        workspace=workspace,
        bundle=args.bundle,
        session=_session(args),
    )
    payload = result.to_mapping()
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(f"build {result.status}: workspace declaration")
        print(f"  bundle: {result.bundle_id}")
        if result.archive:
            print(f"  record: {result.archive}")
        print(f"  items:  {', '.join(result.items)}")
        for error in result.errors:
            # The Weaver operation, then the file to open, then why. Whatever
            # raised it comes last or not at all: a developer whose stored
            # procedure has a syntax error is not helped by reading first that
            # TDS was involved.
            print()
            print(_indented(error.describe()), file=sys.stderr)
    return 0 if result.succeeded else 1


def _indented(text: str, prefix: str = "  ") -> str:
    return "\n".join(prefix + line if line else line for line in text.splitlines())


def handle_doctor(args: argparse.Namespace) -> int:
    """Report what a local build and load needs, and what is missing.

    None of it is required to use Weaver on Fabric. It matters for local
    development, where a missing JDK otherwise surfaces as a Java stack trace.
    """

    from weaver.diagnostics import check_local_spark, platform_summary

    report = check_local_spark()

    if args.json:
        import json

        print(json.dumps(report.as_dict(), indent=2))
        return 0 if report.ok else 1

    print(f"local Spark and Delta on {platform_summary()}\n")
    for check in report.checks:
        print(f"  {check}")
    if report.ok:
        print("\nReady. Run the local tests with:  pytest -m spark")
        return 0
    print()
    for hint in report.hints:
        print(f"  → {hint}")
    # A non-zero status keeps this usable as a gate in a script, but on its own
    # it reads as "your installation is broken" to someone who never wanted
    # local Spark. Say plainly that it is optional.
    print(
        "\nThis reports local Spark only. Weaver on Fabric needs none of it —\n"
        "wipe, install and capacity work without a JVM."
    )
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help()
        return 0

    try:
        return int(handler(args))
    except WeaverError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

"""Shared request grammar for terminal commands and native Notebook workflows."""

from __future__ import annotations

import argparse

import weaver

from .errors import CommandError

CAPACITY_ACTIONS = ("status", "resume", "suspend")

WORKFLOW_DEFAULT_FILE = "workflow.yml"

INITIALISE_DESCRIPTION = "Set up a Weaver project and its Fabric items.\n\nChoose a catalogue Warehouse, Environment, Lakehouse and/or Warehouse.\nMissing items are created.\n\nOptionally add the Parcel or Sales example for the selected items."

WIPE_DESCRIPTION = "Empty physical Fabric items and the catalogue that records them.\n\nNaming targets selects exactly those physical items. Naming none selects the\nestate recorded in the catalogue.\n\nA resolved catalogue is emptied last for Warehouse and Lakehouse selections.\nPass --unbind to keep it and remove its claims for the emptied targets.\n\nSemanticModel-only selections keep the catalogue and remove their own claims.\nRecorded Reports hold no data and are left as they are.\nSemantic wipe retains the Fabric item. --preserve-data-source keeps one Automatic\nSQL / Direct Lake source in a hidden columnless table. Unsupported source and\nconnection forms are refused before mutation."

UNBIND_DESCRIPTION = "Remove the catalogue's claims for physical items. Nothing in Fabric changes.\n\nUse it after deleting an item in Fabric, or to stop managing one. Unbinding an\nitem that still exists asks for confirmation."

TEST_DESCRIPTION = "Run Tests and Assumptions for the selected items.\n\nCatalogue mode runs the validations installed in the Weaver catalogue and\nrecords their results. File mode runs validations from the project folder\nagainst the deployed objects and records nothing. Test uses file mode when\n--file is given or no catalogue is configured."

DOCTOR_DESCRIPTION = "Check Microsoft Fabric connectivity.\n\nName a workspace to probe its items: TDS for a Warehouse; OneLake and Livy for\na Lakehouse. Project configuration is not read.\n\nChecking a Lakehouse starts a Fabric Spark session and can take a minute.\n\nUse --semantic-model NAME to check only Power BI authentication, model definition\nand DAX access through REST."

NON_INTERACTIVE_HELP = "Do not read stdin, wait for a keypress or open browser sign-in. Missing authorisation or required input is an error."


def add_non_interactive(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--non-interactive", action="store_true", help=NON_INTERACTIVE_HELP
    )


def _positive_concurrency(text):
    from weaver.concurrency import validate_concurrency

    try:
        return validate_concurrency(int(text))
    except (ValueError, CommandError) as exc:
        raise argparse.ArgumentTypeError(
            "concurrency must be a positive integer"
        ) from exc


def _add_concurrency_arg(parser):
    parser.add_argument(
        "--concurrency",
        type=_positive_concurrency,
        metavar="N",
        help="Limit total simultaneous execution for this invocation; retain resource limits.",
    )


def _add_initialise_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--workspace", help="Fabric workspace name. It must exist.")
    parser.add_argument(
        "--project-folder",
        dest="project_folder",
        metavar="PATH",
        help="Project folder to create or reuse. Required for unattended setup.",
    )
    parser.add_argument(
        "--catalogue",
        help="Catalogue Warehouse. Defaults to Catalogue for data projects; omitted for native-only projects.",
    )
    parser.add_argument(
        "--environment",
        help="Fabric Environment for Python work. Defaults to Weaver for data projects; omitted for native-only projects.",
    )
    parser.add_argument("--lakehouse", help="Lakehouse for Delta tables and files.")
    parser.add_argument("--warehouse", help="Warehouse for SQL tables and views.")
    parser.add_argument(
        "--semantic-model",
        metavar="NAME",
        help="Add PowerBI/NAME/NAME.tmdl, or name the example's Power BI project. Build creates the SemanticModel.",
    )
    parser.add_argument(
        "--example",
        choices=("Parcel", "Sales"),
        metavar="NAME",
        help="Add the Parcel or Sales example for the selected items.",
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Ask setup questions even when input is not a terminal.",
    )
    add_non_interactive(parser)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the setup without making changes.",
    )
    parser.add_argument("--json", action="store_true", help="Emit the result as JSON.")
    parser.add_argument(
        "--publish-environment",
        action="store_true",
        help="Publish the project Environment after setup.",
    )


def _add_source_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--source",
        metavar="SOURCE",
        help="Project folder, or an abfss location inside a Fabric session. Defaults to the current directory or Notebook Resources.",
    )


def _add_workspace_args(
    parser: argparse.ArgumentParser,
    *,
    include_catalogue: bool = True,
    include_environment: bool = True,
) -> None:
    """Add the explicit values that a Workspace configuration can abbreviate."""
    parser.add_argument("--workspace", help="Fabric Workspace name.")
    parser.add_argument("--workspace-config", help="Workspace configuration file.")
    if include_environment:
        parser.add_argument(
            "--environment",
            help="Fabric Environment name or Workspace/Environment reference.",
        )
    if include_catalogue:
        parser.add_argument(
            "--catalogue",
            help="Where the Weaver catalogue lives, for example Warehouse/Weaver.",
        )


def build_parser(*, callbacks=None) -> argparse.ArgumentParser:
    callbacks = callbacks or {}
    parser = argparse.ArgumentParser(
        prog="weaver",
        description="Build, load, and test Fabric Lakehouse and Warehouse objects.",
    )
    parser.add_argument(
        "--version", action="version", version=f"weaverstack {weaver.__version__}"
    )
    subcommands = parser.add_subparsers(dest="command", metavar="command")
    shell = subcommands.add_parser(
        "session", help="Run multiple commands in one persistent session."
    )
    _add_workspace_args(shell)
    shell.add_argument(
        "--timings",
        action="store_true",
        help="Report Fabric access timings when the session ends.",
    )
    shell.set_defaults(handler=callbacks.get("handle_session"))
    workflow = subcommands.add_parser(
        "workflow", help="Run a named workflow in one session."
    )
    workflow.add_argument("name", help="Workflow name in workflow.yml.")
    workflow.add_argument(
        "--file",
        metavar="PATH",
        help=f"Workflow file. Defaults to ./{WORKFLOW_DEFAULT_FILE}.",
    )
    workflow.add_argument(
        "--timings",
        action="store_true",
        help="Report Fabric access timings after the workflow.",
    )
    workflow.add_argument(
        "--yes",
        action="store_true",
        help="Authorise the workflow and all its commands.",
    )
    add_non_interactive(workflow)
    _add_workspace_args(workflow)
    workflow.set_defaults(handler=callbacks.get("handle_workflow"))
    for name in ("initialise", "initialize"):
        listed = (
            {"help": "Set up a Weaver project and its Fabric items."}
            if name == "initialise"
            else {}
        )
        initialise = subcommands.add_parser(
            name,
            description=INITIALISE_DESCRIPTION,
            formatter_class=argparse.RawDescriptionHelpFormatter,
            **listed,
        )
        _add_initialise_args(initialise)
        initialise.set_defaults(
            handler=callbacks.get("handle_initialise"),
            requires=callbacks.get("_requires_initialise"),
        )
    doctor = subcommands.add_parser(
        "doctor",
        help="Check Microsoft Fabric connectivity.",
        description=DOCTOR_DESCRIPTION,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    doctor.add_argument("--json", action="store_true", help="Emit the result as JSON.")
    doctor.add_argument("--workspace", required=True, help="Fabric workspace to check.")
    doctor.add_argument(
        "--semantic-model",
        metavar="NAME",
        help="Check this semantic model's authentication, definition and DAX access only.",
    )
    add_non_interactive(doctor)
    doctor.set_defaults(
        handler=callbacks.get("handle_doctor"),
        requires=callbacks.get("_requires_doctor"),
    )
    check = subcommands.add_parser(
        "check", help="Check a project folder without contacting Fabric."
    )
    check.add_argument(
        "project_folder",
        metavar="PROJECT_FOLDER",
        nargs="?",
        help="Project folder. Defaults to the current directory.",
    )
    check.add_argument("--json", action="store_true", help="Emit the result as JSON.")
    add_non_interactive(check)
    check.set_defaults(handler=callbacks.get("handle_check"))
    build = subcommands.add_parser(
        "build", help="Build a project's objects into named items."
    )
    build.add_argument(
        "source",
        metavar="SOURCE",
        nargs="?",
        help="Project folder, or an abfss location inside a Fabric session. Defaults to the current directory or Notebook Resources.",
    )
    build.add_argument(
        "--item",
        dest="items",
        action="append",
        metavar="ITEM[=TARGET]",
        help="Weaver item to build. Its physical target comes from workspace configuration; use ITEM=TARGET to supply or override it. Repeat to select multiple items. Naming none builds every configured item.",
    )
    build.add_argument(
        "--bind", dest="retired_bind", action="append", help=argparse.SUPPRESS
    )
    build.add_argument(
        "--target", dest="retired_target", action="append", help=argparse.SUPPRESS
    )
    build.add_argument(
        "--data-source",
        dest="data_sources",
        action="append",
        metavar="EXPRESSION=KIND/NAME",
        help="Substitute a shared M source expression with a Warehouse or Lakehouse.",
    )
    build.add_argument(
        "--bind-data-sources",
        action="store_true",
        help="After deploying a semantic model, bind each of its SQL data sources to the connection that reaches it.",
    )
    build.add_argument(
        "--dry-run",
        action="store_true",
        help="Inspect the Build plan without changing target state.",
    )
    build.add_argument(
        "--bundle-only",
        action="store_true",
        help="Create a deployment bundle without installing it.",
    )
    build.add_argument(
        "--bundle-path",
        metavar="PATH",
        help="Directory to write a bundle created with --bundle-only.",
    )
    _add_concurrency_arg(build)
    build.add_argument("--json", action="store_true", help="Emit the result as JSON.")
    add_non_interactive(build)
    _add_workspace_args(build)
    build.set_defaults(
        handler=callbacks.get("handle_build"),
        requires=callbacks.get("_requires_build"),
        lakehouses=callbacks.get("_build_item_lakehouses"),
    )
    load = subcommands.add_parser(
        "load", help="Load objects installed for the selected items."
    )
    load.add_argument(
        "items",
        nargs="*",
        metavar="ITEM",
        help="Weaver items to load, as Lakehouse/Name, Warehouse/Name or SemanticModel/Name. PowerBI or PowerBI/<project> names the semantic models of the project folder. Naming none loads every installed item.",
    )
    load.add_argument(
        "--item",
        dest="extra_items",
        action="append",
        metavar="ITEM",
        help="Legacy spelling for a positional Weaver item.",
    )
    load.add_argument(
        "--target", dest="retired_target", action="append", help=argparse.SUPPRESS
    )
    load.add_argument(
        "--name",
        dest="names",
        action="append",
        metavar="NAME",
        help="Select installed objects whose whole name matches this regular expression: Tables/Schema.Object or Files/Schema.Object in a Lakehouse, Schema.Object in a Warehouse. A bare Schema.Object is accepted where it names one object. Repeat to select more.",
    )
    _add_source_arg(load)
    _add_concurrency_arg(load)
    load.add_argument(
        "--ancestors",
        action="store_true",
        help="Include recursive upstream loads within the selected items.",
    )
    load.add_argument(
        "--descendants",
        action="store_true",
        help="Include recursive downstream loads within the selected items.",
    )
    load.add_argument(
        "--fault-tolerant",
        action="store_true",
        help="Continue independent branches after a failure.",
    )
    load.add_argument(
        "--reload",
        action="store_true",
        help="Rebuild each selected table: reset its bookmark, empty it, then load. Does not affect unselected tables.",
    )
    load.add_argument(
        "--ignore-stability-threshold",
        action="store_true",
        help="Waive the declared delete and update limits for this load. Row checks, fault tolerance and selection are unchanged.",
    )
    load.add_argument(
        "--stale",
        action="store_true",
        help="Select the objects whose load health is not green.",
    )
    load.add_argument(
        "--as-of",
        metavar="DATETIME",
        help="ISO-8601 instant with a zone, used as the health freshness threshold when selecting objects. Defaults to 24 hours ago. Only valid with --stale.",
    )
    load.add_argument(
        "--dry-run", action="store_true", help="Show the load plan without running it."
    )
    load.add_argument("--json", action="store_true", help="Emit the report as JSON.")
    add_non_interactive(load)
    _add_workspace_args(load)
    load.set_defaults(
        handler=callbacks.get("handle_load"), requires=callbacks.get("_requires_run")
    )
    validate = subcommands.add_parser(
        "test",
        help="Run Tests and Assumptions for the selected items.",
        description=TEST_DESCRIPTION,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    validate.add_argument(
        "items",
        nargs="*",
        metavar="ITEM",
        help="Weaver items to validate, as Lakehouse/Name, Warehouse/Name or SemanticModel/Name. PowerBI or PowerBI/<project> names the semantic models of the project folder. Naming none validates every item.",
    )
    validate.add_argument(
        "--item",
        dest="extra_items",
        action="append",
        metavar="ITEM",
        help="Legacy spelling for a positional Weaver item.",
    )
    validate.add_argument(
        "--target", dest="retired_target", action="append", help=argparse.SUPPRESS
    )
    validate.add_argument(
        "--name",
        dest="names",
        action="append",
        metavar="PATTERN",
        help="Run only validations whose whole Schema.Object matches this regular expression, with their diagnostic rows. Repeat to select more.",
    )
    validate.add_argument(
        "--file",
        dest="files",
        action="append",
        metavar="PATH",
        help="Run validations from source in file mode: a file, a directory or a glob. A file outside the project folder runs against the one item named. Repeat to select more.",
    )
    _add_source_arg(validate)
    _add_concurrency_arg(validate)
    validate.add_argument(
        "--dry-run", action="store_true", help="Show the test plan without running it."
    )
    validate.add_argument(
        "--json", action="store_true", help="Emit the report as JSON."
    )
    add_non_interactive(validate)
    _add_workspace_args(validate)
    validate.set_defaults(
        handler=callbacks.get("handle_test"), requires=callbacks.get("_requires_run")
    )
    report = subcommands.add_parser(
        "health", help="Report the installed estate's load, test and build health."
    )
    report.add_argument(
        "--item",
        dest="items",
        action="append",
        metavar="ITEM",
        help="Weaver item to report on, as Lakehouse/Name, Warehouse/Name or SemanticModel/Name. Repeat to select more than one. Naming none reports on the whole installed estate.",
    )
    report.add_argument(
        "--as-of",
        metavar="DATETIME",
        help="ISO-8601 instant with a zone. A load settled before it reads as stale. Defaults to 24 hours ago.",
    )
    report.add_argument(
        "--no-inventory",
        action="store_true",
        help="Skip the physical inventory check for certified objects.",
    )
    report.add_argument("--json", action="store_true", help="Emit the report as JSON.")
    add_non_interactive(report)
    _add_workspace_args(report, include_environment=False)
    report.set_defaults(
        handler=callbacks.get("handle_health"),
        requires=callbacks.get("_requires_health"),
    )
    wipe = subcommands.add_parser(
        "wipe",
        help="Empty physical Lakehouses, Warehouses or semantic models.",
        description=WIPE_DESCRIPTION,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    wipe.add_argument(
        "targets",
        nargs="*",
        metavar="TARGET",
        help="Physical items to empty: Lakehouse/Name, Warehouse/Name or SemanticModel/Name. Naming none empties the estate the catalogue records.",
    )
    _add_workspace_args(wipe)
    wipe.add_argument(
        "--unbind",
        action="store_true",
        help="Keep the catalogue and remove its claims for the named targets. Requires a catalogue and at least one target.",
    )
    wipe.add_argument(
        "--preserve-data-source",
        action="store_true",
        help="Retain one Automatic SQL / Direct Lake source in a hidden columnless table.",
    )
    wipe.add_argument(
        "--dry-run", action="store_true", help="Show the estate this would empty."
    )
    wipe.add_argument(
        "--yes", action="store_true", help="Authorise the removal without asking."
    )
    wipe.add_argument("--json", action="store_true", help="Emit the result as JSON.")
    add_non_interactive(wipe)
    wipe.set_defaults(
        handler=callbacks.get("handle_wipe"),
        requires=callbacks.get("_requires_wipe"),
        lakehouses=callbacks.get("_physical_target_lakehouses"),
    )
    unbind = subcommands.add_parser(
        "unbind",
        help="Remove the catalogue's claims for physical items.",
        description=UNBIND_DESCRIPTION,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    unbind.add_argument(
        "targets",
        nargs="+",
        metavar="TARGET",
        help="Physical items to unbind: Lakehouse/Name, Warehouse/Name, SemanticModel/Name or Report/Name.",
    )
    _add_workspace_args(unbind, include_environment=False)
    unbind.add_argument(
        "--dry-run", action="store_true", help="Show what this would unbind."
    )
    unbind.add_argument(
        "--yes",
        action="store_true",
        help="Unbind items that still exist without asking.",
    )
    unbind.add_argument("--json", action="store_true", help="Emit the result as JSON.")
    add_non_interactive(unbind)
    unbind.set_defaults(
        handler=callbacks.get("handle_unbind"),
        requires=callbacks.get("_requires_unbind"),
    )
    mirror = subcommands.add_parser(
        "mirror",
        help="Fork another catalogue's installed estate into this workspace's catalogue.",
    )
    mirror.add_argument(
        "--item",
        action="append",
        dest="items",
        metavar="ITEM[=TARGET]",
        help="A logical item to rebind, such as Warehouse/Model or Warehouse/Model=Warehouse/Model_Dev. Repeat to select multiple items. Naming none selects every configured target.",
    )
    mirror.add_argument(
        "--no-item",
        action="store_true",
        help="Fork the catalogue and rebind no physical item.",
    )
    mirror.add_argument(
        "--mirror",
        metavar="CATALOGUE",
        dest="mirror_source",
        help="Catalogue to fork, for example Warehouse/Weaver. Overrides mirror: in workspace configuration.",
    )
    _add_workspace_args(mirror)
    mirror.add_argument(
        "--yes",
        action="store_true",
        help="Authorise emptying the destinations without asking.",
    )
    mirror.add_argument("--json", action="store_true", help="Emit the result as JSON.")
    add_non_interactive(mirror)
    mirror.set_defaults(
        handler=callbacks.get("handle_mirror"),
        requires=callbacks.get("_requires_mirror"),
    )
    install = subcommands.add_parser(
        "install", help="Install a previously built deployment bundle."
    )
    install.add_argument(
        "bundle", metavar="BUNDLE", help="Bundle directory or .weaver.zip archive."
    )
    install.add_argument("--json", action="store_true", help="Emit the report as JSON.")
    add_non_interactive(install)
    install.set_defaults(
        handler=callbacks.get("handle_install"),
        requires=callbacks.get("_requires_install"),
    )
    fabric = subcommands.add_parser(
        "fabric", help="Manage Fabric items, Environments, and capacity."
    )
    fabric_commands = fabric.add_subparsers(dest="fabric_command", metavar="command")
    fabric.set_defaults(
        handler=callbacks.get("_group_help", lambda parser: None)(fabric)
    )
    environment = fabric_commands.add_parser(
        "environment", help="Manage Fabric Environments."
    )
    environment_commands = environment.add_subparsers(
        dest="environment_command", metavar="command"
    )
    environment.set_defaults(
        handler=callbacks.get("_group_help", lambda parser: None)(environment)
    )
    environment_publish = environment_commands.add_parser(
        "publish", help="Publish Weaver into a Fabric Environment."
    )
    environment_publish.add_argument(
        "environment_ref",
        metavar="ENVIRONMENT",
        nargs="?",
        help="Fabric Environment name or Workspace/Environment reference.",
    )
    environment_publish.add_argument(
        "--path",
        metavar="DIRECTORY",
        help="Local <Name>.Environment definition directory. It names the Environment and supplies the complete definition.",
    )
    environment_publish.add_argument(
        "--dev",
        action="store_true",
        help="Supply Weaver as a wheel built from this checkout.",
    )
    environment_publish.add_argument(
        "--json", action="store_true", help="Emit the result as JSON."
    )
    add_non_interactive(environment_publish)
    _add_workspace_args(
        environment_publish, include_catalogue=False, include_environment=False
    )
    environment_publish.set_defaults(
        handler=callbacks.get("handle_environment_publish"),
        requires=callbacks.get("_requires_rest"),
    )
    notebook = fabric_commands.add_parser(
        "notebook", help="Deploy or run a Fabric notebook."
    )
    notebook_commands = notebook.add_subparsers(
        dest="notebook_command", metavar="command"
    )
    notebook.set_defaults(
        handler=callbacks.get("_group_help", lambda parser: None)(notebook)
    )
    notebook_push = notebook_commands.add_parser(
        "push", help="Create or update a notebook definition."
    )
    notebook_push.add_argument("source", help="Local .py or .ipynb notebook source.")
    notebook_push.add_argument(
        "--name", help="Fabric display name. Defaults to the filename."
    )
    notebook_push.add_argument("--description")
    notebook_push.add_argument("--json", action="store_true")
    add_non_interactive(notebook_push)
    _add_workspace_args(notebook_push, include_catalogue=False)
    notebook_push.set_defaults(handler=callbacks.get("handle_notebook_push"))
    notebook_run = notebook_commands.add_parser(
        "run", help="Run a deployed notebook in Fabric."
    )
    notebook_run.add_argument("name", help="Fabric Notebook display name.")
    notebook_run.add_argument(
        "--lakehouse", help="Default Lakehouse for the notebook session."
    )
    notebook_run.add_argument("--no-wait", action="store_true")
    notebook_run.add_argument("--timeout", type=float, default=7200.0)
    notebook_run.add_argument("--poll-interval", type=float, default=10.0)
    notebook_run.add_argument("--json", action="store_true")
    add_non_interactive(notebook_run)
    _add_workspace_args(notebook_run)
    notebook_run.set_defaults(handler=callbacks.get("handle_notebook_run"))
    capacity = fabric_commands.add_parser(
        "capacity", help="Resume, suspend, or report a Fabric capacity."
    )
    capacity.add_argument("action", choices=CAPACITY_ACTIONS)
    capacity.add_argument("--resource-group", required=True)
    capacity.add_argument("--capacity-name", required=True)
    capacity.add_argument(
        "--subscription-id",
        help="Azure subscription ID when more than one subscription is available.",
    )
    add_non_interactive(capacity)
    capacity.set_defaults(handler=callbacks.get("handle_capacity"))
    return parser

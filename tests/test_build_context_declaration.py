"""How `weaver.build` decides which workspace and Weaver catalogue it means.

The public build has to work from a Fabric notebook, where a caller reasonably
supplies only a repository and its bindings, and from a desktop, where nothing
can be inferred and a missing value has to be said out loud. One resolution
order serves both:

    explicit argument → typed Workspace → configuration → notebook context

The part worth testing is the precedence, because every step of it is a value
that would otherwise be plausible. A configured catalogue and an attached
default Lakehouse are both real Lakehouses; picking the wrong one writes a
catalogue into somewhere that works, and is wrong in a way nothing complains
about.

The other half is where failure lands. Missing context must be a Weaver sentence
before any session starts, not a Py4J traceback from inside one.
"""

from __future__ import annotations

import sys
import types

import pytest
from support.weaver_test import weaver_test

import weaver
import weaver.operations.build
import weaver.operations.workspace
from weaver.errors import BuildError, CommandError
from weaver.workspaces import Workspace


@pytest.fixture
def in_notebook(monkeypatch):
    """A Fabric session whose context names a workspace and a default Lakehouse."""

    module = types.ModuleType("notebookutils")
    module.runtime = types.SimpleNamespace(
        context={
            "currentWorkspaceName": "Analytics",
            "currentWorkspaceId": "ws-id",
            "defaultLakehouseId": "lh-id",
            "defaultLakehouseName": "AttachedWeaver",
        }
    )
    monkeypatch.setitem(sys.modules, "notebookutils", module)
    return module


class Halt(Exception):
    """Raised in place of doing the build, once resolution has happened."""


@pytest.fixture
def captured(monkeypatch):
    """Stop each build at the platform seam and report what it resolved."""

    seen = {}

    def capture(name):
        def _stop(workspace, **kwargs):
            seen["mode"] = name
            seen["workspace"] = workspace
            seen["bindings"] = kwargs.get("bindings")
            raise Halt(name)

        return _stop

    # One seam, because there is one build: what the Session answers is what
    # differs between a notebook and a desktop, not which algorithm runs.
    monkeypatch.setattr(weaver.operations.build, "_run_build", capture("build"))
    # Preflight is a different claim. That a build proves its items exist before
    # opening anything, and has its own tests below.
    monkeypatch.setattr(weaver.operations.build, "_preflight", lambda *a, **k: None)
    return seen


@pytest.fixture
def repository(tmp_path):
    from test_item_repository_declaration import _schema, _table, _write

    root = tmp_path / "Estate"
    _write(root, "Lakehouse/Sales/schemas/DWG.yml", _schema("DWG"))
    _write(root, "Lakehouse/Sales/DWG__Customer.py", _table("DWG.Customer"))
    return root


def _build(repository, **kwargs):
    return weaver.build(
        str(repository), items="Lakehouse/Sales=Lakehouse/Sales_LH", **kwargs
    )


@weaver_test()
def test_bundle_path_refuses_an_already_populated_directory(tmp_path):
    """`--bundle-path` cannot leave an old manifest or payload in place."""

    output = tmp_path / "release"
    output.mkdir()
    (output / "plan.yml").write_text("old bundle", encoding="utf-8")

    with pytest.raises(BuildError, match="must not exist or must be empty"):
        weaver.operations.build._bundle_output(output)


@pytest.fixture
def public_shortcut_build(tmp_path, monkeypatch):
    from support.sessions import given_session
    from support.workspaces import given_resolver, given_workspace
    from test_item_repository_declaration import _schema, _table, _write

    from weaver.build_bundle import load_bundle
    from weaver.locations import Location
    from weaver.store import FilesystemStore
    from weaver.targets import ItemRef

    root = tmp_path / "Estate"
    _write(root, "Lakehouse/Play/schemas/Source.yml", _schema("Source"))
    _write(
        root,
        "Lakehouse/Play/shortcuts.py",
        "from weaver import Shortcut\n\n"
        "Source__RawData = Shortcut(\n"
        '    shortcut_type="folder",\n'
        '    target_type="physical",\n'
        '    target="Lakehouse/Reference/Files/RawData",\n'
        ")\n",
    )
    _write(root, "Lakehouse/Archive/schemas/History.yml", _schema("History"))
    _write(
        root,
        "Lakehouse/Archive/History__Event.py",
        _table("History.Event"),
    )

    workspace = given_workspace(catalogue="Warehouse/Weaver")
    store = FilesystemStore()
    resolver = given_resolver(
        workspace=workspace,
        lakehouses=("Play_LH", "Reference"),
        warehouses=("Weaver",),
        root=tmp_path / "fabric",
    )
    for item in ("Play_LH", "Reference"):
        store.make_directory(resolver.files_root(ItemRef(item)))
        store.make_directory(resolver.tables_root(ItemRef(item)))
    store.make_directory(resolver.files_root(ItemRef("Reference")) / "RawData")

    monkeypatch.setattr(weaver.operations.build, "_preflight", lambda *a, **k: None)

    def run(bundle_name="bundle"):
        class EmptySql:
            def query(self, _statement, parameters=None):
                return []

        output = tmp_path / bundle_name
        with given_session(
            workspace=workspace, resolver=resolver, store=store
        ) as session:
            session.sql_executor = lambda *_args, **_kwargs: EmptySql()
            result = weaver.build(
                str(root),
                items="Lakehouse/Play=Lakehouse/Play_LH",
                session=session,
                bundle_only=True,
                bundle_path=output,
            )
        return load_bundle(Location(result.bundle_path), store=FilesystemStore())

    return types.SimpleNamespace(run=run)


@weaver_test()
def test_public_build_resolves_a_physical_folder_shortcut(public_shortcut_build):
    """Public state preparation freezes the source before planning the bundle."""

    bundle = public_shortcut_build.run()
    assert any(
        action.kind == "create_shortcut"
        for _sequence, _batch, action in bundle.plan.actions()
    )
    assert not any(
        node.reason == "shortcut_unsupported" for node in bundle.plan.omitted_nodes
    )


@weaver_test()
def test_public_build_refuses_a_selected_shortcut_without_a_resolved_source(
    public_shortcut_build, monkeypatch
):
    """A selected object cannot disappear from a successful public build."""

    monkeypatch.setattr(
        "weaver.build_bundle.workflow.read_shortcut_sources",
        lambda *_args, **_kwargs: {},
    )

    with pytest.raises(
        BuildError,
        match="selected object.*physical target was not resolved",
    ):
        public_shortcut_build.run()


@weaver_test()
def test_public_partial_build_records_an_unbound_item(public_shortcut_build):
    """An object outside the requested bindings remains a scope omission."""

    bundle = public_shortcut_build.run()
    omitted = {node.node_id: node.reason for node in bundle.plan.omitted_nodes}
    assert omitted["Lakehouse/Archive/History.Event"] == "target_unbound"


# --- notebook inference -------------------------------------------------------


@weaver_test()
def test_a_notebook_infers_the_current_workspace(in_notebook, captured, repository):
    """The catalogue is given so that only the workspace is in question."""

    with pytest.raises(Halt):
        _build(repository, catalogue="Warehouse/Weaver")

    assert captured["workspace"].workspace == "Analytics"


# --- explicit values win ------------------------------------------------------


@weaver_test()
def test_an_operation_given_a_resolved_workspace_says_to_open_a_session(
    repository, tmp_path
):
    """Operations take names. A resolved Workspace goes through a Session.

    One way in rather than two: a Workspace argument and a Session argument
    would both carry a context, and an operation given each would have to pick
    between them.
    """

    workspace = Workspace(workspace="Demo", catalogue="Warehouse/Configured")

    with pytest.raises(CommandError, match="weaver.session"):
        _build(repository, workspace=workspace)


@weaver_test()
def test_an_explicit_catalogue_overrides_the_sessions_workspace(
    captured, repository, desktop_credential
):
    """The Session supplies the context; an argument still outranks it."""

    with weaver.session(workspace="Demo", catalogue="Warehouse/Configured") as session:
        with pytest.raises(Halt):
            _build(repository, session=session, catalogue="Warehouse/Chosen")

    assert captured["workspace"].catalogue == "Warehouse/Chosen"


@weaver_test()
def test_the_sessions_workspace_supplies_the_catalogue_when_no_argument_does(
    captured, repository, desktop_credential
):
    with weaver.session(workspace="Demo", catalogue="Warehouse/Configured") as session:
        with pytest.raises(Halt):
            _build(repository, session=session)

    assert captured["workspace"].catalogue == "Warehouse/Configured"


@weaver_test()
def test_a_desktop_caller_needs_no_workspace_object(captured, repository, tmp_path):
    """`workspace=` and `catalogue=` alone are a complete desktop context."""

    with pytest.raises(Halt):
        _build(repository, workspace="Analytics", catalogue="Warehouse/Weaver")

    assert captured["mode"] == "build"
    assert captured["workspace"].workspace == "Analytics"
    assert captured["workspace"].catalogue == "Warehouse/Weaver"


# --- and missing context is a sentence ----------------------------------------


@weaver_test()
def test_no_context_outside_fabric_names_what_to_supply(repository, monkeypatch):
    monkeypatch.delitem(sys.modules, "notebookutils", raising=False)
    monkeypatch.setattr(
        weaver.operations.workspace,
        "_current_fabric_workspace",
        lambda: (_ for _ in ()).throw(
            CommandError("give workspace or workspace_config outside a Fabric notebook")
        ),
    )

    with pytest.raises(CommandError, match="outside a Fabric notebook"):
        _build(repository)


@weaver_test()
def test_a_workspace_without_a_catalogue_says_both_ways_to_give_one(
    repository, tmp_path
):
    with pytest.raises(CommandError) as raised:
        _build(repository, workspace="Demo")

    message = str(raised.value)
    assert "catalogue=" in message
    assert "workspace configuration" in message


@weaver_test()
def test_a_resolved_workspace_and_a_configuration_file_is_refused_by_the_session(
    tmp_path, desktop_credential
):
    """The same rule, now stated once where a Workspace is accepted at all."""

    config = tmp_path / "ws.yml"
    config.write_text("workspace: Other\n", encoding="utf-8")

    with pytest.raises(CommandError, match="nothing to add"):
        weaver.session(
            workspace=Workspace(workspace="Demo", catalogue="Warehouse/Weaver"),
            workspace_config=config,
        )


# --- where a Spark session would attach ---------------------------------------
#
# Fabric creates a Livy session against a Lakehouse, so a host that crosses
# needs the id of one. It comes from the bindings the build was given, which is
# why a workspace configuring no Lakehouses can still build into one.


@weaver_test()
def test_a_build_offers_the_lakehouse_its_target_named(repository, captured):
    """No configured targets, and the build still says where Spark could live."""

    from weaver.sessions.testing import TestSession

    workspace = Workspace(workspace="Demo", catalogue="Warehouse/Weaver")
    session = TestSession(workspace=workspace)

    with pytest.raises(Halt):
        _build(repository, session=session)

    assert not workspace.targets
    assert session.scope(workspace).spark_home == "Sales_LH"


@weaver_test()
def test_a_warehouse_only_build_offers_none(captured, tmp_path):
    """Nothing to attach, and nothing that needs attaching."""

    from test_item_repository_declaration import _schema, _write

    from weaver.sessions.testing import TestSession

    root = tmp_path / "Reporting"
    _write(root, "Warehouse/Reporting/schemas/DWG.yml", _schema("DWG"))
    workspace = Workspace(workspace="Demo", catalogue="Warehouse/Weaver")
    session = TestSession(workspace=workspace)

    with pytest.raises(Halt):
        weaver.build(
            str(root),
            items="Warehouse/Reporting=Warehouse/Reporting_WH",
            session=session,
        )

    assert session.scope(workspace).spark_home is None


# --- the Livy session is never reached by a build that cannot succeed ----------


@weaver_test()
def test_a_failed_preflight_does_not_create_a_livy_session(
    repository, monkeypatch, tmp_path
):
    """The whole point of preflight, stated as the call that must not happen."""

    from weaver.fabric import preflight as preflight_module

    def refuse(*args, **kwargs):
        raise preflight_module.PreflightError(
            "Fabric build preflight failed in workspace 'Analytics':\n"
            "- Weaver catalogue 'Weaver' was not found"
        )

    monkeypatch.setattr(preflight_module, "preflight_fabric_targets", refuse)

    import weaver.fabric as fabric

    def explode(*args, **kwargs):
        raise AssertionError("a Livy session was created after preflight failed")

    monkeypatch.setattr(fabric.LivySession, "for_workspace", explode)

    with pytest.raises(preflight_module.PreflightError, match="was not found"):
        _build(
            repository,
            workspace="Analytics",
            catalogue="Warehouse/Weaver",
            environment="WeaverEnv",
        )


@weaver_test()
def test_a_desktop_build_needs_no_environment(repository, monkeypatch):
    """A build's Spark SQL imports nothing, so it needs no published wheel.

    Refusing here put a publish in front of every build, including a
    Warehouse-only one that starts no Spark session at all.
    """

    from weaver.fabric import preflight as preflight_module

    seen = {}

    def record(*args, **kwargs):
        seen.update(kwargs)
        raise Halt()

    monkeypatch.setattr(preflight_module, "preflight_fabric_targets", record)

    with pytest.raises(Halt):
        _build(repository, workspace="Analytics", catalogue="Warehouse/Weaver")

    assert seen["environment"] is None


@weaver_test()
def test_a_repository_error_is_reported_before_any_fabric_call(tmp_path, monkeypatch):
    """Repository errors come first: they need no workspace to be true."""

    from weaver.fabric import preflight as preflight_module

    def explode(*args, **kwargs):
        raise AssertionError("Fabric was contacted before the repository parsed")

    monkeypatch.setattr(preflight_module, "preflight_fabric_targets", explode)

    empty = tmp_path / "Empty"
    empty.mkdir()

    with pytest.raises(BuildError):
        weaver.build(
            str(empty),
            items="Lakehouse/Sales=Lakehouse/Sales_LH",
            workspace="Analytics",
            catalogue="Warehouse/Weaver",
            environment="WeaverEnv",
        )

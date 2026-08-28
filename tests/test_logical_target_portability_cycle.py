"""One repository, one command sequence, two environments.

The claim the logical target interface exists for: switching from development to
production changes the workspace configuration and nothing else. The same
repository, the same logical target names, the same build, load and test.

.. code-block:: text

    build   logical item  →  workspace configuration  →  physical item
    load    logical item  →  _.Installation           →  physical item
    test    logical item  →  _.Installation           →  physical item

Two owners, and the difference between them is what this file proves. A build
consumes deployment configuration, because it is what establishes the binding. A
load and a test consume installed state, because once an item is built the
catalogue is where it lives. Set the two to disagree and each follows its own.
"""

from __future__ import annotations

import pytest
from factories import (
    ITEM,
    installed_catalogue,
    item_bindings,
    lakehouse_table,
    single_document_repository,
)
from support.weaver_test import weaver_test

import weaver
import weaver.operations.build
from weaver.declaration.model import WeaverItemId
from weaver.run import RunState
from weaver.sessions.testing import TestSession
from weaver.workspaces import Workspace

#: The logical item the repository below authors. One name, both environments.
LANDING = WeaverItemId.parse(ITEM)

DEV = """\
workspace: Analytics Dev
catalogue: Warehouse/Weaver_Dev

targets:
  {item}: Landing_Dev
"""

PROD = """\
workspace: Analytics
catalogue: Warehouse/Weaver

targets:
  {item}: Landing
"""

#: The sequence a developer types. Identical in both environments apart from the
#: configuration it selects.
SEQUENCE = (
    "build ./repository --target {item} --workspace-config {config}",
    "load --target {item} --workspace-config {config}",
    "test --target {item} --workspace-config {config}",
)


class Halt(Exception):
    """Raised in place of doing the build, once its targets are normalised."""


@pytest.fixture
def bindings(monkeypatch):
    """Stop each build at the platform seam and report what it bound."""

    seen = {}

    def stop(workspace, **kwargs):
        seen["workspace"] = workspace
        seen["bindings"] = kwargs["bindings"]
        raise Halt("build")

    monkeypatch.setattr(weaver.operations.build, "_run_build", stop)
    monkeypatch.setattr(weaver.operations.build, "_preflight", lambda *a, **k: None)
    return seen


@pytest.fixture
def root(tmp_path):
    """The repository tree, and the parsed repository built from it."""

    from types import SimpleNamespace

    path = tmp_path / "repository"
    parsed = single_document_repository(
        path, documents={"DWG__Customer.py": lakehouse_table("DWG.Customer")}
    )
    return SimpleNamespace(path=path, repository=parsed)


def _config(tmp_path, text: str, name: str):
    path = tmp_path / name
    path.write_text(text.format(item=ITEM), encoding="utf-8")
    return path


def _physical(seen) -> dict[str, str]:
    """Each bound logical item and the physical item it builds into."""

    return {
        str(binding.item): binding.target.item.name
        for binding in seen["bindings"].entries
    }


# --- build follows the configuration it was given -----------------------------


@weaver_test()
def test_the_same_target_builds_into_each_environments_own_item(
    tmp_path, root, bindings
):
    """One logical name, two physical destinations, one command."""

    built = {}
    for name, text in (("dev", DEV), ("prod", PROD)):
        with pytest.raises(Halt):
            weaver.build(
                str(root.path),
                targets=[ITEM],
                workspace_config=_config(tmp_path, text, f"{name}.yml"),
                session=TestSession(workspace=Workspace(workspace="Nowhere")),
            )
        built[name] = (bindings["workspace"], dict(_physical(bindings)))

    dev_workspace, dev = built["dev"]
    prod_workspace, prod = built["prod"]

    assert dev[ITEM] == "Landing_Dev"
    assert prod[ITEM] == "Landing"
    # The workspace and the catalogue travel with the configuration too.
    assert (dev_workspace.workspace, dev_workspace.catalogue) == (
        "Analytics Dev",
        "Warehouse/Weaver_Dev",
    )
    assert (prod_workspace.workspace, prod_workspace.catalogue) == (
        "Analytics",
        "Warehouse/Weaver",
    )


@weaver_test()
def test_naming_no_target_builds_every_configured_item(tmp_path, root, bindings):
    """Configuration is a target list as well as a mapping."""

    with pytest.raises(Halt):
        weaver.build(
            str(root.path),
            workspace_config=_config(tmp_path, DEV, "dev.yml"),
            session=TestSession(workspace=Workspace(workspace="Nowhere")),
        )

    assert _physical(bindings)[ITEM] == "Landing_Dev"


@weaver_test()
def test_an_explicit_physical_target_overrides_the_configured_one(
    tmp_path, root, bindings
):
    """The one place a caller may say where a build lands."""

    with pytest.raises(Halt):
        weaver.build(
            str(root.path),
            targets=[f"{ITEM}=Lakehouse/Landing_Scratch"],
            workspace_config=_config(tmp_path, DEV, "dev.yml"),
            session=TestSession(workspace=Workspace(workspace="Nowhere")),
        )

    assert _physical(bindings)[ITEM] == "Landing_Scratch"


# --- load and test follow the catalogue ---------------------------------------


def _installed(repository, *, physical: str):
    """The catalogue a build of this repository into ``physical`` would leave."""

    return installed_catalogue(repository, item_bindings((ITEM, physical)))


def _configured(tmp_path, *, physical: str) -> Workspace:
    """A workspace configuration that disagrees with the catalogue."""

    from weaver.config import load_workspace

    path = tmp_path / "disagreeing.yml"
    path.write_text(
        f"workspace: Analytics\ncatalogue: Warehouse/Weaver\n\ntargets:\n"
        f"  {ITEM}: {physical}\n",
        encoding="utf-8",
    )
    return load_workspace(path)


@weaver_test()
def test_a_load_uses_the_installed_target_and_not_the_configured_one(tmp_path, root):
    """The key proof. Configuration says one thing, the catalogue another.

    A build consumes configuration because it decides where to deploy. A load
    consumes the catalogue because the item is already deployed, and the estate
    is where it actually is.
    """

    from weaver.operations.load import run_load

    workspace = _configured(tmp_path, physical="Landing_Config")
    state = RunState(
        catalogue=_installed(root.repository, physical="Landing_Installed")
    )
    session = TestSession(workspace=workspace)

    report = run_load(
        session, workspace=workspace, state=state, requested=(LANDING,), dry_run=True
    )

    # What the caller asked for, in the caller's own vocabulary.
    assert report.requested == (ITEM,)
    # And where it ran, which the catalogue supplied.
    assert {node.physical_target for node in report.nodes} == {
        "Lakehouse/Landing_Installed"
    }
    assert session.scope(workspace).spark_home == "Landing_Installed"


@weaver_test()
def test_a_test_uses_the_installed_target_and_not_the_configured_one(tmp_path, root):
    """Test reads the same authority a load reads, for the same reason."""

    from weaver.operations.test import run_test

    workspace = _configured(tmp_path, physical="Landing_Config")
    state = RunState(
        catalogue=_installed(root.repository, physical="Landing_Installed")
    )
    session = TestSession(workspace=workspace)

    run_test(
        session, workspace=workspace, state=state, requested=(LANDING,), dry_run=True
    )

    assert session.scope(workspace).spark_home == "Landing_Installed"


@weaver_test()
def test_no_run_target_may_carry_a_physical_half(tmp_path, root):
    """Once an item is built, the catalogue is authoritative.

    A caller cannot send a load somewhere other than where the item is, and a
    workspace configuration cannot either, which the two tests above prove.
    """

    from weaver.errors import CommandError

    workspace = _configured(tmp_path, physical="Landing_Config")
    session = TestSession(workspace=workspace)

    for operation in (weaver.load, weaver.test):
        with pytest.raises(CommandError, match="read from the Weaver catalogue"):
            operation([f"{ITEM}=Lakehouse/Anything"], session=session)


# --- the notebook API is the same interface -----------------------------------


@weaver_test()
def test_the_notebook_api_names_the_same_logical_items(tmp_path, root, bindings):
    """Not a CLI translation. The public functions take the logical identities.

    Asserted at the public functions, because that is the interface a notebook
    calls and argparse is not in front of it.
    """

    with pytest.raises(Halt):
        weaver.build(
            str(root.path),
            targets=[ITEM],
            workspace_config=_config(tmp_path, DEV, "dev.yml"),
            session=TestSession(workspace=Workspace(workspace="Nowhere")),
        )

    assert _physical(bindings)[ITEM] == "Landing_Dev"

    from weaver.operations.load import run_load
    from weaver.operations.test import run_test

    workspace = _configured(tmp_path, physical="Landing_Dev")
    state = RunState(catalogue=_installed(root.repository, physical="Landing_Dev"))

    for run in (run_load, run_test):
        report = run(
            TestSession(workspace=workspace),
            workspace=workspace,
            state=state,
            requested=(LANDING,),
            dry_run=True,
        )
        assert report is not None


# --- one composition, two environments ----------------------------------------


@weaver_test()
def test_one_composition_serves_both_environments(tmp_path):
    """The sequence is the program. The configuration is the environment.

    Parsed rather than run: what is claimed is that the command text is the same
    text, and that each verb reaches the operation with the same logical scope.
    """

    from weaver_cli.compose import composition_words
    from weaver_cli.main import build_parser

    parser = build_parser()
    parsed = {}
    for name, text in (("dev", DEV), ("prod", PROD)):
        config = _config(tmp_path, text, f"{name}.yml")
        parsed[name] = [
            parser.parse_args(composition_words(line.format(item=ITEM, config=config)))
            for line in SEQUENCE
        ]

    for dev, prod in zip(parsed["dev"], parsed["prod"], strict=True):
        # The same logical scope, verb by verb.
        assert dev.targets == prod.targets == [ITEM]
        # And the only difference between the two lines.
        assert dev.workspace_config != prod.workspace_config

    # Nothing in parsing or displaying a composition resolves a physical target.
    from weaver_cli.main import command_lakehouses

    assert [command_lakehouses(one) for one in parsed["dev"]] == [(), (), ()]

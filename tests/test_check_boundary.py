"""Source-only project checking."""

from __future__ import annotations

import json
from pathlib import Path
from shutil import copytree

import pytest
from support.weaver_test import weaver_test

from weaver.operations.check import check
from weaver_cli.main import build_parser, command_requirements, handle_check


def _project(root: Path) -> Path:
    fixture = Path(__file__).parent / "fixtures" / "build-lakehouse-item"
    copytree(fixture, root)
    return root


@weaver_test()
def test_check_parses_a_valid_project_without_running_authored_python(tmp_path):
    root = _project(tmp_path / "project")
    source = root / "Lakehouse" / "Raw" / "Tables" / "DWG__Customer.py"
    source.write_text(
        source.read_text(encoding="utf-8")
        + "\nraise RuntimeError('must not execute')\n",
        encoding="utf-8",
    )

    result = check(root)

    # A Location normalises to "/" on every platform, so compare in that form.
    assert result.project_folder == root.as_posix()


@weaver_test()
def test_check_accepts_unrelated_item_content(tmp_path):
    root = _project(tmp_path / "project")
    (root / "Lakehouse" / "Raw" / "unexpected.txt").write_text("x", encoding="utf-8")

    assert check(root).project_folder == root.as_posix()


@weaver_test()
def test_check_command_declares_no_fabric_resources_and_renders_success(
    tmp_path, capsys
):
    root = _project(tmp_path / "project")
    args = build_parser().parse_args(["check", str(root)])

    assert command_requirements(args) == frozenset()
    assert handle_check(args) == 0
    assert capsys.readouterr().out == "Project valid.\n"


@weaver_test()
def test_check_json_returns_the_project_folder(tmp_path, capsys):
    root = _project(tmp_path / "project")
    args = build_parser().parse_args(["check", str(root), "--json"])

    assert handle_check(args) == 0
    assert json.loads(capsys.readouterr().out) == {
        "status": "succeeded",
        "project_folder": root.as_posix(),
    }


@weaver_test()
def test_check_command_adapts_parser_error_to_retry_status(tmp_path, capsys):
    root = _project(tmp_path / "project")
    (root / "Sales.Customer.sql").write_text(
        "/* Table ID: Sales.Customer */\nselect 1;\n", encoding="utf-8"
    )
    args = build_parser().parse_args(["check", str(root)])

    assert handle_check(args) == 1
    assert "error:" in capsys.readouterr().err


@weaver_test()
def test_check_json_keeps_an_expected_error_machine_readable(tmp_path, capsys):
    root = _project(tmp_path / "project")
    (root / "Sales.Customer.sql").write_text(
        "/* Table ID: Sales.Customer */\nselect 1;\n", encoding="utf-8"
    )
    args = build_parser().parse_args(["check", str(root), "--json"])

    assert handle_check(args) == 1

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["status"] == "failed"
    assert payload["error"]["message"]
    assert captured.err == ""


@weaver_test()
def test_a_folder_that_is_not_a_weaver_project_says_so(tmp_path, capsys):
    """The folder the command was pointed at, not the bystander inside it."""

    export = tmp_path / "project" / "FabricExport" / "Thing" / "schemas"
    export.mkdir(parents=True)
    (export / "Sales.yml").write_text(
        "Schema ID: Sales\nDescription: Sales objects.\n", encoding="utf-8"
    )
    args = build_parser().parse_args(["check", str(tmp_path / "project")])

    assert handle_check(args) == 1

    printed = capsys.readouterr().err
    assert "not a Weaver project" in printed
    assert "Lakehouse/<Name>/" in printed
    assert "FabricExport" not in printed


# --- what a malformed project says --------------------------------------------


TABLE = "Lakehouse/Raw/Tables/DWG__Customer.py"


def _document(root: Path) -> Path:
    return root / "Lakehouse" / "Raw" / "Tables" / "DWG__Customer.py"


def _failed(root: Path, capsys) -> str:
    args = build_parser().parse_args(["check", str(root)])
    assert handle_check(args) == 1
    printed = capsys.readouterr().err
    assert "Traceback" not in printed
    return printed


@weaver_test()
def test_malformed_metadata_names_its_file_and_where_in_it(tmp_path, capsys):
    """The file a user opens, and a position inside the block they wrote.

    The parser only ever sees the extracted block, so the coordinates are
    labelled as positions in it rather than presented as file lines.
    """

    root = _project(tmp_path / "project")
    document = _document(root)
    document.write_text(
        '"""\nTable ID: DWG.Customer\n\nDescription: Customers.\n'
        "\nSchema:\n  Customer id: string\n   Customer name: string\n"
        '"""\n\nfrom weaver import Table\n',
        encoding="utf-8",
    )

    printed = _failed(root, capsys)

    assert TABLE in printed
    assert "Metadata line" in printed
    assert "column" in printed
    assert printed.count(TABLE) == 1


@weaver_test()
def test_a_malformed_project_fails_and_the_repaired_one_passes(tmp_path, capsys):
    """A check is something a user runs again after editing."""

    root = _project(tmp_path / "project")
    document = _document(root)
    original = document.read_text(encoding="utf-8")
    document.write_text('"""\n- not a mapping\n"""\n', encoding="utf-8")

    _failed(root, capsys)
    document.write_text(original, encoding="utf-8")

    assert check(root).project_folder == root.as_posix()


@weaver_test()
def test_sql_the_parser_cannot_get_through_names_its_file(tmp_path, capsys):
    """A parser limit is a source failure, reported against the source."""

    import weaver.declaration.source as source_module

    root = _project(tmp_path / "project")
    original = source_module.analyse_sql

    def exhausted(body):
        raise RecursionError("maximum recursion depth exceeded")

    source_module.analyse_sql = exhausted
    try:
        printed = _failed(root, capsys)
    finally:
        source_module.analyse_sql = original

    assert "DWG.ActiveCustomer.sql" in printed
    assert "SQL could not be analysed" in printed
    assert "maximum recursion depth exceeded" in printed


@weaver_test()
def test_deeply_nested_sql_is_analysed_rather_than_refused(tmp_path):
    """The real input, not an injected failure.

    Weaver lifts sqlparse's grouping ceiling for authored SQL, so a query being
    large is not by itself a reason it cannot be checked.
    """

    root = _project(tmp_path / "project")
    nested = "select 1" + "".join(
        f" union all select {number}" for number in range(2, 600)
    )
    view = root / "Lakehouse" / "Raw" / "Tables" / "DWG.Wide.sql"
    view.write_text(
        f"/*\nView ID: DWG.Wide\n\nDescription: Wide.\n\nLineage: Raw.\n\n"
        f"Dependencies: []\n*/\n"
        f"{nested};\n",
        encoding="utf-8",
    )

    assert check(root).project_folder == root.as_posix()


@weaver_test()
def test_a_source_file_that_cannot_be_copied_names_it(tmp_path, capsys):
    """Injected at the copy boundary, so the claim holds wherever it runs and
    needs no permission a test runner may or may not have."""

    import shutil

    import weaver.store as store_module

    root = _project(tmp_path / "project")
    original = store_module.FilesystemStore.copy_to_local

    def refuse(self, source, destination):
        raise shutil.Error(
            [
                (str(source.path / TABLE), str(destination), "Permission denied"),
                (str(source.path / "Lakehouse" / "Raw" / "x.py"), "d", "No such file"),
            ]
        )

    store_module.FilesystemStore.copy_to_local = refuse
    try:
        printed = _failed(root, capsys)
    finally:
        store_module.FilesystemStore.copy_to_local = original

    assert "could not be copied" in printed
    assert TABLE in printed
    assert "Permission denied" in printed
    # Reported as the project names them, not as the snapshot does.
    assert str(tmp_path) not in printed.replace(root.as_posix(), "")


@weaver_test()
def test_a_long_list_of_copy_failures_is_bounded(tmp_path, capsys):
    import shutil

    import weaver.store as store_module
    from weaver.build_bundle.workflow import COPY_FAILURE_LIMIT

    root = _project(tmp_path / "project")
    original = store_module.FilesystemStore.copy_to_local
    failures = [
        (str(root / f"file{number}.py"), "d", "Permission denied")
        for number in range(COPY_FAILURE_LIMIT + 3)
    ]

    def refuse(self, source, destination):
        raise shutil.Error(failures)

    store_module.FilesystemStore.copy_to_local = refuse
    try:
        printed = _failed(root, capsys)
    finally:
        store_module.FilesystemStore.copy_to_local = original

    assert printed.count("Permission denied") == COPY_FAILURE_LIMIT
    assert "and 3 more file(s)" in printed


@weaver_test()
def test_a_source_read_failure_names_the_file_it_could_not_read(tmp_path, capsys):
    import weaver.store as store_module

    root = _project(tmp_path / "project")
    original = store_module.FilesystemStore.copy_to_local

    def refuse(self, source, destination):
        raise PermissionError(13, "Permission denied", str(source.path / TABLE))

    store_module.FilesystemStore.copy_to_local = refuse
    try:
        printed = _failed(root, capsys)
    finally:
        store_module.FilesystemStore.copy_to_local = original

    assert "could not be read" in printed
    assert TABLE in printed
    assert "Permission denied" in printed


@weaver_test()
def test_an_offline_check_reaches_no_fabric_capability(tmp_path, monkeypatch):
    """Every failure above is reported without a credential, a resolver or a
    Session, which is what makes a check something to run on every save."""

    import weaver.resolution as resolution

    root = _project(tmp_path / "project")
    monkeypatch.setattr(
        resolution,
        "resolver_for",
        lambda workspace: pytest.fail("check must reach no resolver"),
    )
    monkeypatch.setattr(
        resolution,
        "store_for",
        lambda workspace: pytest.fail("check must reach no target store"),
    )
    _document(root).write_text('"""\n- not a mapping\n"""\n', encoding="utf-8")

    from weaver.errors import WeaverError

    with pytest.raises(WeaverError):
        check(root)


@weaver_test()
def test_build_reports_a_malformed_source_the_way_check_does(tmp_path):
    """One preparation, so a project Build cannot read is a source failure there
    too, reported before a workspace is resolved or a target is touched."""

    from support.sessions import given_session
    from support.workspaces import given_workspace

    import weaver
    from weaver.declaration.model import WeaverItemId
    from weaver.errors import MetadataError
    from weaver.workspaces import TargetDeclaration

    root = _project(tmp_path / "project")
    _document(root).write_text('"""\n- not a mapping\n"""\n', encoding="utf-8")
    workspace = given_workspace(
        targets={WeaverItemId.parse("Lakehouse/Raw"): TargetDeclaration("Sales_LH")}
    )

    with pytest.raises(MetadataError) as refused:
        weaver.build(root, session=given_session(workspace=workspace))

    assert "Lakehouse/Raw/Tables/DWG__Customer.py" in str(refused.value)

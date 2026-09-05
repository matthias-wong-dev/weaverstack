"""Which crossings `weaver doctor` proves, and what it says when one fails.

Doctor answers one question: can Weaver get there. Not whether the estate is
healthy, which is `weaver health`, and not whether a repository parses, which is
`weaver check`.

What it can prove depends on what it was given. With nothing it proves sign-in
and the Fabric REST API together, because a listing that comes back means a
token was issued and the control plane accepted it. With a project's own
configuration it knows the items, so it opens each endpoint that project is
reached through and no others.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.errors import CommandError
from weaver.operations import doctor as module
from weaver.operations.doctor import FAILED, OK, doctor

LAKEHOUSE_AND_WAREHOUSE = """\
workspace: Weaver Example
catalogue: Warehouse/Catalogue

targets:
  Lakehouse/Landing: Landing
  Warehouse/Curated: Curated
"""

WAREHOUSE_ONLY = """\
workspace: Weaver Example
catalogue: Warehouse/Catalogue

targets:
  Warehouse/Curated: Curated
"""

LAKEHOUSE_ONLY = """\
workspace: Weaver Example
catalogue: Warehouse/Catalogue

targets:
  Lakehouse/Landing: Landing
"""


class _Client:
    """A Fabric REST client that answers, or refuses."""

    def __init__(self, *, reachable=True):
        self.reachable = reachable
        self.paths: list[str] = []

    def paged(self, path, **_):
        self.paths.append(path)
        if not self.reachable:
            raise CommandError("Fabric REST is not reachable.")
        return [{"id": "ws-1", "displayName": "Weaver Example"}]


class _Session:
    """A Session that records the crossings it was asked to make."""

    workspace = None
    closed = False

    def __init__(self, *, failing=(), client=None):
        self.tsql: list[str] = []
        self.spark: list[str] = []
        self.files: list[str] = []
        self.failing = set(failing)
        #: What this Session's resolver answers REST with.
        self.client = client if client is not None else _Client()

    def query_tsql(self, statement, *, target, workspace=None, parameters=None):
        name = str(target.warehouse.name)
        self.tsql.append(name)
        if "tds" in self.failing:
            raise CommandError(f"Cannot open a connection to {name}.")
        return [(1,)]

    def execute_spark_sql(self, statement, *, workspace=None, **_):
        self.spark.append(statement)
        if "livy" in self.failing:
            raise CommandError("A Spark session could not be started.")
        return [(1,)]

    def resolver(self, workspace=None):
        return self

    def files_root(self, item):
        return f"onelake://{item.name}/Files"

    def store(self, workspace=None):
        return self

    def exists(self, location):
        self.files.append(str(location))
        if "onelake" in self.failing:
            raise CommandError("OneLake refused the read.")
        return True


@pytest.fixture
def workspace_found(monkeypatch):
    """A workspace that resolves."""

    from weaver.fabric import resources

    found = type("Workspace", (), {"id": "ws-1", "name": "Weaver Example"})
    monkeypatch.setattr(resources, "find_workspace", lambda name, client=None: found())
    return found


def _project(tmp_path, monkeypatch, text: str):
    (tmp_path / "workspace-config.yml").write_text(text, encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    return tmp_path / "workspace-config.yml"


def _named(report):
    return [check.name for check in report.checks]


# --- what can be proven with nothing to point at -------------------------------


@weaver_test()
def test_signing_in_and_reaching_fabric_are_one_check(tmp_path, monkeypatch):
    """A listing that comes back means a token was issued and accepted."""

    monkeypatch.chdir(tmp_path)
    client = _Client()

    report = doctor(client=client)

    assert _named(report) == ["Fabric REST"]
    assert report.succeeded is True
    assert client.paths == ["workspaces"]


@weaver_test()
def test_an_unreachable_fabric_stops_before_anything_else(tmp_path, monkeypatch):
    """Nothing after it could succeed, and each failure would repeat the cause."""

    monkeypatch.chdir(tmp_path)

    report = doctor(client=_Client(reachable=False))

    assert _named(report) == ["Fabric REST"]
    assert report.succeeded is False
    assert report.failures[0].detail == "Fabric REST is not reachable."
    assert "signed in" in report.failures[0].remedy


@weaver_test()
def test_nothing_is_claimed_about_tds_or_livy_without_a_target(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    report = doctor(client=_Client())

    assert not [name for name in _named(report) if "TDS" in name or "Spark" in name]


# --- with a workspace ----------------------------------------------------------


@weaver_test()
def test_a_named_workspace_is_resolved(tmp_path, monkeypatch, workspace_found):
    monkeypatch.chdir(tmp_path)

    report = doctor(workspace="Weaver Example", client=_Client(), session=_Session())

    assert _named(report) == ["Fabric REST", "Workspace Weaver Example"]
    assert report.workspace == "Weaver Example"


@weaver_test()
def test_a_workspace_that_does_not_resolve_says_what_to_check(tmp_path, monkeypatch):
    from weaver.fabric import resources

    monkeypatch.chdir(tmp_path)

    def missing(name, client=None):
        raise CommandError(f"Workspace {name!r} was not found.")

    monkeypatch.setattr(resources, "find_workspace", missing)

    report = doctor(workspace="Nowhere", client=_Client())

    assert report.succeeded is False
    assert report.failures[0].name == "Workspace Nowhere"
    assert "has access to it" in report.failures[0].remedy


# --- with a project ------------------------------------------------------------


@weaver_test()
def test_a_project_is_checked_on_every_endpoint_it_uses(
    tmp_path, monkeypatch, workspace_found
):
    _project(tmp_path, monkeypatch, LAKEHOUSE_AND_WAREHOUSE)
    session = _Session()

    report = doctor(client=_Client(), session=session)

    assert _named(report) == [
        "Fabric REST",
        "Workspace Weaver Example",
        "Warehouse/Catalogue TDS",
        "Warehouse/Curated TDS",
        "Lakehouse/Landing OneLake",
        "Spark session",
    ]
    assert session.tsql == ["Catalogue", "Curated"]
    assert session.spark == ["select 1"]
    assert report.succeeded is True


@weaver_test()
def test_the_session_client_is_what_reaches_fabric_rest(monkeypatch):
    """A caller holding a Session must not construct a client of its own.

    The Session's carries its renewing token and its telemetry, and inside
    Fabric it is the notebook identity.
    """

    from weaver.fabric import resources

    session = _Session()
    seen = []
    monkeypatch.setattr(
        resources, "find_workspace", lambda name, client=None: seen.append(client)
    )

    doctor(workspace="Weaver Example", session=session)

    assert session.client.paths == ["workspaces"]
    assert seen == [session.client]


@weaver_test()
def test_a_session_with_no_workspace_still_proves_sign_in(monkeypatch):
    """`doctor` runs before a workspace is known, and a Session without one has
    no resolver to ask."""

    built = _Client()

    class _Unscoped(_Session):
        def resolver(self, workspace=None):
            raise CommandError("A Workspace is required for this command.")

    monkeypatch.setattr(module, "FabricClient", lambda *a, **k: built, raising=False)
    monkeypatch.setattr("weaver.fabric.client.FabricClient", lambda *a, **k: built)

    report = doctor(session=_Unscoped())

    assert report.checks[0].name == "Fabric REST"
    assert report.checks[0].passed
    assert built.paths == ["workspaces"]


@weaver_test()
def test_a_warehouse_project_starts_no_spark_session(
    tmp_path, monkeypatch, workspace_found
):
    """The objects are T-SQL and the catalogue is a Warehouse, so Livy is not used."""

    _project(tmp_path, monkeypatch, WAREHOUSE_ONLY)
    session = _Session()

    report = doctor(client=_Client(), session=session)

    assert session.spark == []
    assert "Spark session" not in _named(report)


@weaver_test()
def test_a_lakehouse_project_still_checks_tds(tmp_path, monkeypatch, workspace_found):
    """The Weaver catalogue is a Warehouse, so every project reaches TDS."""

    _project(tmp_path, monkeypatch, LAKEHOUSE_ONLY)
    session = _Session()

    doctor(client=_Client(), session=session)

    assert session.tsql == ["Catalogue"]
    assert session.spark == ["select 1"]


@weaver_test()
def test_the_project_beside_the_command_is_the_one_checked(
    tmp_path, monkeypatch, workspace_found
):
    """Doctor reads `workspace-config.yml` the way every other command now does."""

    _project(tmp_path, monkeypatch, WAREHOUSE_ONLY)

    report = doctor(client=_Client(), session=_Session())

    assert report.workspace == "Weaver Example"


@weaver_test()
def test_a_session_supplies_the_workspace_it_is_open_on(
    tmp_path, monkeypatch, workspace_found
):
    """Inside `weaver session` the workspace is the session's, as it is elsewhere."""

    from weaver.config import parse_workspace

    monkeypatch.chdir(tmp_path)
    session = _Session()
    session.workspace = parse_workspace(
        {
            "workspace": "Weaver Example",
            "catalogue": "Warehouse/Catalogue",
            "targets": {"Warehouse/Curated": "Curated"},
        }
    )

    report = doctor(client=_Client(), session=session)

    assert report.workspace == "Weaver Example"
    assert session.tsql == ["Catalogue", "Curated"]


@weaver_test()
def test_a_named_configuration_outranks_the_session(
    tmp_path, monkeypatch, workspace_found
):
    from weaver.config import parse_workspace

    configuration = _project(tmp_path, monkeypatch, LAKEHOUSE_ONLY)
    session = _Session()
    session.workspace = parse_workspace(
        {"workspace": "Somewhere Else", "catalogue": "Warehouse/Other"}
    )

    report = doctor(
        workspace_config=str(configuration), client=_Client(), session=session
    )

    assert report.workspace == "Weaver Example"


@weaver_test()
def test_a_failed_crossing_names_itself_and_the_others_still_run(
    tmp_path, monkeypatch, workspace_found
):
    """One unreachable endpoint says nothing about the rest, so the rest are asked."""

    _project(tmp_path, monkeypatch, LAKEHOUSE_AND_WAREHOUSE)
    session = _Session(failing={"livy"})

    report = doctor(client=_Client(), session=session)

    assert report.succeeded is False
    assert [check.status for check in report.checks] == [OK, OK, OK, OK, OK, FAILED]
    failure = report.failures[0]
    assert failure.name == "Spark session"
    assert failure.detail == "A Spark session could not be started."
    assert "capacity is running" in failure.remedy


@weaver_test()
def test_a_transport_error_is_reported_rather_than_raised(
    tmp_path, monkeypatch, workspace_found
):
    """A check that let a library's own error escape would report on nothing after it."""

    _project(tmp_path, monkeypatch, WAREHOUSE_ONLY)

    class _Broken(_Session):
        def query_tsql(self, statement, *, target, workspace=None, parameters=None):
            raise TimeoutError("the connection timed out")

    report = doctor(client=_Client(), session=_Broken())

    assert report.succeeded is False
    assert report.failures[0].detail == "TimeoutError: the connection timed out"


@weaver_test()
def test_the_result_serialises_whole():
    from weaver.operations.doctor import Check, DoctorReport

    report = DoctorReport(
        checks=(Check("Fabric REST", OK), Check("Spark session", FAILED, "no", "try")),
        workspace="Weaver Example",
    )

    assert report.to_mapping() == {
        "workspace": "Weaver Example",
        "succeeded": False,
        "checks": [
            {"name": "Fabric REST", "status": OK, "detail": None, "remedy": None},
            {
                "name": "Spark session",
                "status": FAILED,
                "detail": "no",
                "remedy": "try",
            },
        ],
    }


@weaver_test()
def test_doctor_probes_are_the_cheapest_question_each_surface_answers():
    assert module.TDS_PROBE == "select 1"
    assert module.LIVY_PROBE == "select 1"

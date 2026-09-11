"""Which estate a wipe empties, and what it does with the catalogue.

Two decisions. Target selection names the physical items: the ones given, or
the estate the catalogue's ``_.Installation`` rows describe. Catalogue
disposition says what happens to the catalogue: removed by default, preserved
with its claims cleaned under ``--unbind``, untouched where none resolves.

A plan settles both, and a wipe empties the plan it was handed. The plan a
caller shows is the plan it runs.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver import plan_wipe
from weaver import wipe as public_wipe
from weaver.errors import CommandError
from weaver.locations import Location
from weaver.operations.wipe import (
    LEAVE,
    REMOVE,
    UNBIND,
    WipeReport,
    WipeTarget,
    installed_targets,
)
from weaver.sessions.testing import TestSession


def _session() -> TestSession:
    """A recording Session with a store, which a Lakehouse wipe asks for."""

    return TestSession(store=object())


def _operations():
    import sys

    import weaver.operations.wipe  # noqa: F401 - imported for sys.modules

    return sys.modules["weaver.operations.wipe"]


class _Installations:
    """A catalogue holding the ``_.Installation`` rows a test declares.

    The ``CatalogueConnection`` shape, so :func:`read_table` projects and reads
    these rows exactly as it does over TDS.
    """

    def __init__(self, rows):
        self.declared = list(rows)
        self.statements = []

    def columns_of(self, table):
        if table.name != "Installation":
            return None
        return {
            column.public_name.casefold(): column.public_name
            for column in table.columns
        }

    def rows(self, statement):
        self.statements.append(statement)
        return [
            {
                "item_type": item_type,
                "item_name": item_name,
                "target_name": target,
                "weaver_version": "0.9.0",
                "declaration_signature": "signature",
            }
            for item_type, item_name, target in self.declared
        ]

    def execute(self, statement):
        self.statements.append(statement)


def _plan(*targets, catalogue="Warehouse/Weaver", workspace="Analytics", **rest):
    return plan_wipe(
        targets, workspace=workspace, catalogue=catalogue, session=_session(), **rest
    )


def _names(plan) -> list[str]:
    return [str(target) for target in plan.targets]


# --- selecting targets --------------------------------------------------------


@weaver_test()
def test_named_targets_are_exactly_what_a_wipe_selects():
    """No expansion. Two targets are two items, plus the catalogue."""

    plan = _plan("Lakehouse/Landing", "Warehouse/Curated")

    assert _names(plan) == [
        "Lakehouse/Landing",
        "Warehouse/Curated",
        "Warehouse/Weaver",
    ]


@weaver_test()
def test_a_resolved_catalogue_is_removed_by_default_and_goes_last():
    plan = _plan("Lakehouse/Landing")

    assert plan.catalogue_action == REMOVE
    assert _names(plan)[-1] == "Warehouse/Weaver"
    assert plan.is_catalogue(plan.targets[-1])
    assert plan.unbound == ()


@weaver_test()
def test_the_catalogue_appears_once_when_it_is_also_a_named_target():
    plan = _plan("Warehouse/Weaver", "Lakehouse/Landing")

    assert _names(plan) == ["Lakehouse/Landing", "Warehouse/Weaver"]


@weaver_test()
def test_a_lakehouse_of_the_catalogues_name_is_a_different_item():
    """Level-three identity is workspace, type and name."""

    plan = _plan("Lakehouse/Weaver")

    assert _names(plan) == ["Lakehouse/Weaver", "Warehouse/Weaver"]
    assert plan.is_catalogue(plan.targets[0]) is False


@weaver_test()
def test_no_resolvable_catalogue_leaves_the_selection_alone():
    plan = _plan("Lakehouse/Landing", catalogue=None)

    assert plan.catalogue_action == LEAVE
    assert plan.catalogue is None
    assert _names(plan) == ["Lakehouse/Landing"]


# --- unbind -------------------------------------------------------------------


@weaver_test()
def test_unbind_preserves_the_catalogue_and_names_the_claims():
    plan = _plan("Lakehouse/Landing", unbind=True)

    assert plan.catalogue_action == UNBIND
    assert _names(plan) == ["Lakehouse/Landing"]
    assert plan.unbound == ("Lakehouse/Landing",)


@weaver_test()
def test_unbind_without_a_catalogue_is_refused_before_anything_is_emptied():
    with pytest.raises(CommandError, match="resolved none"):
        _plan("Lakehouse/Landing", catalogue=None, unbind=True)


@weaver_test()
def test_unbind_naming_the_catalogue_itself_is_refused():
    with pytest.raises(CommandError, match="also names it as a target"):
        _plan("Warehouse/Weaver", unbind=True)


@weaver_test()
def test_unbind_with_no_target_selection_is_refused():
    with pytest.raises(CommandError, match="needs them named"):
        _plan(unbind=True)


# --- what an internal caller asks for -----------------------------------------


@weaver_test()
def test_a_named_disposition_empties_one_item_and_no_estate():
    """Mirror empties one Warehouse. A catalogue is not an estate index there."""

    plan = _plan("Warehouse/Sales_Dev", catalogue_action=UNBIND)

    assert _names(plan) == ["Warehouse/Sales_Dev"]
    assert plan.unbound == ("Warehouse/Sales_Dev",)


@weaver_test()
def test_emptying_the_catalogue_itself_unbinds_nothing_from_it():
    plan = _plan("Warehouse/Weaver", catalogue_action=UNBIND)

    assert _names(plan) == ["Warehouse/Weaver"]
    assert plan.unbound == ()


@weaver_test()
def test_a_disposition_and_an_unbind_flag_that_disagree_are_refused():
    with pytest.raises(CommandError, match="catalogue_action says"):
        _plan("Warehouse/Sales_Dev", unbind=True, catalogue_action=LEAVE)


@weaver_test()
def test_an_unknown_disposition_names_the_ones_there_are():
    with pytest.raises(CommandError, match="catalogue_action is one of"):
        _plan("Warehouse/Sales_Dev", catalogue_action="destroy")


@weaver_test()
def test_mirror_asks_for_the_physical_only_disposition():
    """Read off the wiring, so the estate semantics cannot reach mirror."""

    import inspect
    import sys

    import weaver.operations.mirror  # noqa: F401 - imported for sys.modules

    source = inspect.getsource(sys.modules["weaver.operations.mirror"])

    assert source.count("catalogue_action=UNBIND") == 2
    assert "plan_wipe" not in source


# --- estate discovery ---------------------------------------------------------


@weaver_test()
def test_the_estate_comes_from_the_catalogue_installations():
    catalogue = _Installations(
        [
            ("Lakehouse", "Sales", "Landing_Dev"),
            ("Warehouse", "Reporting", "Curated_Dev"),
            # Two logical items in one target are one physical item to empty.
            ("Lakehouse", "Inventory", "Landing_Dev"),
        ]
    )

    assert [str(target) for target in installed_targets(catalogue)] == [
        "Lakehouse/Landing_Dev",
        "Warehouse/Curated_Dev",
    ]


@weaver_test()
def test_an_estate_read_names_the_installation_table():
    catalogue = _Installations([("Lakehouse", "Sales", "Landing_Dev")])
    installed_targets(catalogue)

    assert any("[_].[Installation]" in each for each in catalogue.statements)


@weaver_test()
def test_a_catalogue_with_no_installations_leaves_only_itself(monkeypatch):
    monkeypatch.setattr(_operations(), "_installed_estate", lambda *_a, **_k: ())

    assert _names(_plan()) == ["Warehouse/Weaver"]


@weaver_test()
def test_a_discovered_estate_still_puts_the_catalogue_last(monkeypatch):
    monkeypatch.setattr(
        _operations(),
        "_installed_estate",
        lambda *_a, **_k: (
            WipeTarget.parse("Warehouse/Weaver"),
            WipeTarget.parse("Lakehouse/Landing_Dev"),
        ),
    )

    assert _names(_plan()) == ["Lakehouse/Landing_Dev", "Warehouse/Weaver"]


@weaver_test()
def test_an_unscoped_wipe_reads_installations_and_not_configuration(
    tmp_path, monkeypatch
):
    """What configuration declares is what a build would install."""

    configuration = tmp_path / "workspace-config.yml"
    configuration.write_text(
        "workspace: Analytics\ncatalogue: Warehouse/Weaver\ntargets:\n"
        "  Warehouse/Sales: Declared_Only\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        _operations(),
        "_installed_estate",
        lambda *_a, **_k: (WipeTarget.parse("Warehouse/Installed"),),
    )

    plan = plan_wipe(workspace_config=configuration, session=_session())

    assert _names(plan) == ["Warehouse/Installed", "Warehouse/Weaver"]


@weaver_test()
def test_a_wipe_with_no_targets_and_no_catalogue_says_what_it_needs():
    with pytest.raises(CommandError, match="needs targets or a Weaver catalogue"):
        plan_wipe(workspace="Analytics", session=_session())


# --- executing the plan -------------------------------------------------------


def _emptied(monkeypatch, removed=("Sales",), area="delta"):
    """Record which targets a wipe reached, touching no physical item."""

    operations = _operations()
    reached = []

    def one(target, *_args, **kwargs):
        reached.append(str(target))
        return (
            WipeReport(
                f"{area}:{target}",
                Location(f"/tmp/local/{target.physical_name}"),
                removed,
                kwargs["dry_run"],
            ),
        )

    monkeypatch.setattr(operations, "_wipe_one", one)
    return operations, reached


@weaver_test()
def test_a_wipe_empties_the_plan_it_was_given(monkeypatch):
    _operations_module, reached = _emptied(monkeypatch)
    plan = _plan("Lakehouse/Landing")

    result = public_wipe(plan=plan, session=_session())

    assert reached == ["Lakehouse/Landing", "Warehouse/Weaver"]
    assert result.plan is plan


@weaver_test()
def test_a_plan_and_a_selection_together_are_refused():
    with pytest.raises(CommandError, match="a plan or a target selection"):
        public_wipe("Lakehouse/Landing", plan=_plan("Warehouse/Curated"))


@weaver_test()
def test_a_removed_catalogue_unbinds_nothing(monkeypatch):
    operations, _reached = _emptied(monkeypatch)
    monkeypatch.setattr(
        operations,
        "_unbind_physical_targets",
        lambda *_a, **_k: pytest.fail("claims were deleted from an emptied catalogue"),
    )

    result = public_wipe(plan=_plan("Lakehouse/Landing"), session=_session())

    assert result.unbound is None


@weaver_test()
def test_unbind_removes_the_claims_for_the_targets_it_emptied(monkeypatch):
    operations, reached = _emptied(monkeypatch)
    asked = []
    monkeypatch.setattr(
        operations,
        "_unbind_physical_targets",
        lambda _workspace, targets, **_k: (
            asked.append(tuple(map(str, targets))) or {"targets": []}
        ),
    )

    result = public_wipe(
        plan=_plan("Lakehouse/Landing", unbind=True), session=_session()
    )

    assert reached == ["Lakehouse/Landing"]
    assert asked == [("Lakehouse/Landing",)]
    assert result.unbound == {"targets": []}


@weaver_test()
def test_a_dry_run_removes_nothing_and_deletes_no_claim(monkeypatch):
    operations, _reached = _emptied(monkeypatch)
    monkeypatch.setattr(
        operations,
        "_unbind_physical_targets",
        lambda *_a, **_k: pytest.fail("a dry run deleted claims"),
    )

    result = public_wipe(
        plan=_plan("Lakehouse/Landing", unbind=True),
        session=_session(),
        dry_run=True,
    )

    assert result.dry_run
    assert all(report.dry_run for report in result.reports)


# --- one result line per physical item ----------------------------------------


@weaver_test()
def test_one_coherent_line_per_physical_item(monkeypatch):
    _emptied(monkeypatch, removed=("Sales", "Reporting", "shortcut:Tables/External"))

    result = public_wipe(plan=_plan("Lakehouse/Landing"), session=_session())

    assert [item.target for item in result.items] == [
        "Lakehouse/Landing",
        "Warehouse/Weaver",
    ]
    assert result.items[0].counts == {"tables": 2, "shortcuts": 1}
    assert "2 tables" in result.items[0].describe()
    assert "1 shortcuts" in result.items[0].describe()


@weaver_test()
def test_a_files_area_counts_folders(monkeypatch):
    _emptied(monkeypatch, removed=("notes.txt", "Sales"), area="folder")

    result = public_wipe(plan=_plan("Lakehouse/Landing"), session=_session())

    assert result.items[0].counts == {"folders": 2}


@weaver_test()
def test_a_warehouse_counts_nothing_rather_than_one_placeholder(monkeypatch):
    """A placeholder name became a count of one for a Warehouse holding many."""

    operations = _operations()
    monkeypatch.setattr(
        operations,
        "_wipe_one",
        lambda target, *_a, **k: (
            WipeReport(
                str(target),
                Location(f"warehouse://{target.physical_name}"),
                (),
                k["dry_run"],
            ),
        ),
    )

    result = public_wipe(plan=_plan("Warehouse/Curated"), session=_session())

    assert result.items[0].counts == {}
    assert result.items[0].describe().split() == ["Warehouse/Curated", "emptied"]


@weaver_test()
def test_a_preserved_catalogue_reports_itself_as_preserved(monkeypatch):
    operations, _reached = _emptied(monkeypatch)
    monkeypatch.setattr(
        operations, "_unbind_physical_targets", lambda *_a, **_k: {"targets": []}
    )

    result = public_wipe(
        plan=_plan("Lakehouse/Landing", unbind=True), session=_session()
    )

    catalogue = result.items[-1]
    assert catalogue.target == "Warehouse/Weaver"
    assert catalogue.outcome == "preserved"
    assert catalogue.unbound is True
    assert "claims unbound" in catalogue.describe()


@weaver_test()
def test_the_json_result_carries_the_plan_and_the_items(monkeypatch):
    _emptied(monkeypatch)

    payload = public_wipe(
        plan=_plan("Lakehouse/Landing"), session=_session()
    ).to_mapping()

    assert payload["plan"]["catalogue_action"] == REMOVE
    assert payload["plan"]["targets"][-1] == {
        "target": "Warehouse/Weaver",
        "catalogue": True,
    }
    assert [item["target"] for item in payload["items"]] == [
        "Lakehouse/Landing",
        "Warehouse/Weaver",
    ]
    # Estate level. The low-level per-area removals stay off the result payload.
    assert "reports" not in payload


# --- the question a person answers --------------------------------------------


@weaver_test()
def test_the_preflight_names_items_and_no_objects_inside_them():
    described = _plan("Lakehouse/Landing", "Warehouse/Curated").describe()

    assert "Wipe on Analytics" in described
    assert "Lakehouse/Landing" in described
    assert "Warehouse/Weaver  emptied last" in described
    for inventory in ("Tables/", "Files/", "abfss://", "dbo.", "shortcut:"):
        assert inventory not in described


@weaver_test()
def test_the_preflight_marks_which_item_is_the_catalogue():
    described = _plan("Lakehouse/Landing").describe()

    assert "Warehouse/Weaver  catalogue" in described


@weaver_test()
def test_the_preflight_says_where_claims_are_unbound():
    described = _plan("Lakehouse/Landing", unbind=True).describe()

    assert "preserved; claims for Lakehouse/Landing unbound" in described

"""The authored file an install failure should name.

A build can involve hundreds of repository files, and by the time a physical
action fails, the only spellings left are the deployed ones:

.. code-block:: text

    _/Load/Sales__Customer.py
    [_].[Load Sales.Customer]

Neither is a file anybody has open. So the repository-relative path is carried
from where the authored file is parsed, all the way to the failure:

.. code-block:: text

    SourceDocument.relative_path
        → RuntimeArtefact.source_path
            → InstallAction.source_path
                → plan.yml, and back
                    → ActionResult.source_path
                        → "Source: Lakehouse/Sales/Sales__Customer.py"

**Carried, never reconstructed.** Deriving it later from an action id, a
procedure name or a deployed path would be a guess presented as evidence — and
the guess is not always possible: a Spark SQL table is authored as ``.sql`` and
deployed as ``.py`` under a different name.

Deliberately file-level. No line numbers, no source maps, no mapping a remote
SQL error position back into authored text.
"""

from __future__ import annotations

import hashlib

import pytest
import yaml

from weaver.build_bundle import (
    BoundTarget,
    BuildBatch,
    BuildPlan,
    BuildSelection,
    BuildSequence,
    Impact,
    InstallAction,
    compute_bundle_id,
    plan_from_yaml,
    plan_to_yaml,
)

PAYLOAD = b"create or alter procedure [_].[Load Sales.CustomerRevenue] as\nselect 1\n"
SOURCE = "Warehouse/Reporting/Sales.CustomerRevenue.sql"


def _action(**overrides) -> InstallAction:
    return InstallAction(
        **{
            "id": "load-Warehouse-Reporting-Sales.CustomerRevenue",
            "kind": "build_procedure",
            "resource_node_id": "Warehouse/Reporting/_.Load Sales.CustomerRevenue",
            "executor": "tsql",
            "payload": "payload/load.sql",
            "payload_sha256": hashlib.sha256(PAYLOAD).hexdigest(),
            "source_path": SOURCE,
            **overrides,
        }
    )


# --- the action carries it ----------------------------------------------------


def test_an_action_carries_the_authored_file_it_came_from():
    assert _action().source_path == SOURCE


def test_an_action_with_no_authored_source_says_so():
    """An alias, an endpoint refresh, a prune, a catalogue publication: None is
    the truthful answer, not the nearest-looking file."""

    assert _action(source_path=None).source_path is None


def test_the_key_is_absent_rather_than_null_when_there_is_no_source():
    """plan.yml is what bundle_id hashes. A key written on every action would
    change the id of every bundle that has no authored source to name."""

    assert "source_path" not in _action(source_path=None).to_mapping()
    assert _action().to_mapping()["source_path"] == SOURCE


# --- it survives the bundle ---------------------------------------------------


def _plan(action: InstallAction) -> BuildPlan:
    target = BoundTarget(id="warehouse-Reporting", kind="warehouse", item_id="Reporting")
    return BuildPlan(
        format_version=1,
        bundle_id="",
        repository_name="MyRepo",
        repository_signature="sig-abc",
        targets=(target,),
        sequences=(
            BuildSequence(
                number=60,
                description="install load artefacts",
                batches=(
                    BuildBatch(id="b-load", target_id=target.id, actions=(action,)),
                ),
            ),
        ),
        selection=BuildSelection(Impact((), (), ()), (), (), ()),
        omitted_nodes=(),
    )


def test_the_authored_path_round_trips_through_plan_yaml():
    """The requirement that stops provenance working only in memory.

    A bundle can be written, archived and reopened — and once Build is
    decomposed, the action that fails may be executed by a different transport
    from the one that planned it.
    """

    restored = plan_from_yaml(plan_to_yaml(_plan(_action())))

    (sequence,) = restored.sequences
    (batch,) = sequence.batches
    (action,) = batch.actions
    assert action.source_path == SOURCE


def test_an_action_without_a_source_round_trips_as_having_none():
    restored = plan_from_yaml(plan_to_yaml(_plan(_action(source_path=None))))

    assert restored.sequences[0].batches[0].actions[0].source_path is None


def test_the_authored_path_is_part_of_what_the_bundle_id_identifies():
    """Two bundles differing only in which file an action came from are not the
    same bundle: the second would report the first one's source on failure."""

    one = _plan(_action())
    other = _plan(_action(source_path="Warehouse/Reporting/Somewhere.Else.sql"))

    assert compute_bundle_id(one) != compute_bundle_id(other)


def test_a_plan_written_before_provenance_existed_still_loads():
    """Absent means absent. An older archive is readable, and simply cannot say
    which file an action came from."""

    mapping = _plan(_action()).to_mapping()
    for sequence in mapping["sequences"]:
        for batch in sequence["batches"]:
            for action in batch["actions"]:
                action.pop("source_path", None)

    restored = plan_from_yaml(yaml.safe_dump(mapping))

    assert restored.sequences[0].batches[0].actions[0].source_path is None


# --- what a failure says ------------------------------------------------------


def test_a_failure_names_the_operation_then_the_file_then_the_reason():
    """The developer-facing error describes the Weaver operation.

    Not ``SqlError``, not ``via TDS``: the infrastructure is how the problem was
    found out, not what went wrong, so it comes last or not at all.
    """

    from weaver.operations import BuildFailure

    described = BuildFailure(
        action_id="object-warehouse-reporting-sales-customerrevenue",
        error_type="SqlError",
        message="Incorrect syntax near 'from'.",
        artefact="Warehouse/Reporting/Sales.CustomerRevenue",
        source_path=SOURCE,
    ).describe()

    assert described.splitlines() == [
        "Error installing Warehouse/Reporting/Sales.CustomerRevenue",
        f"Source: {SOURCE}",
        "Incorrect syntax near 'from'.",
    ]


def test_a_failure_with_no_authored_source_still_names_what_failed():
    from weaver.operations import BuildFailure

    described = BuildFailure(
        action_id="alias-Lakehouse-Reporting-DWG.Customer",
        error_type="AliasError",
        message="the shortcut did not become addressable",
        artefact="Lakehouse/Reporting/DWG.Customer",
    ).describe()

    assert "Source:" not in described
    assert described.startswith("Error installing Lakehouse/Reporting/DWG.Customer")


def test_a_failure_falls_back_to_the_action_id_when_it_has_no_artefact():
    from weaver.operations import BuildFailure

    described = BuildFailure(
        action_id="publish-registry", error_type="BuildError", message="no"
    ).describe()

    assert described.startswith("Error installing publish-registry")


# --- the rule that keeps it honest --------------------------------------------


def test_nothing_reconstructs_an_authored_path_from_a_deployed_one():
    """Read off the source, because this is the failure mode the contract is
    for: a path derived at install time looks right and is a guess.

    A Spark SQL table is authored as ``Sales.OrderSummary.sql`` and deployed as
    ``Sales__OrderSummary.py``. Any rule that turned the second back into the
    first would be inventing the mapping this field exists to carry.
    """

    from pathlib import Path

    root = Path(__file__).resolve().parents[1] / "src" / "weaver"
    offenders = []
    for module in sorted(root.rglob("*.py")):
        source = module.read_text(encoding="utf-8")
        for line in source.splitlines():
            stripped = line.strip()
            if stripped.startswith("#") or "source_path" not in stripped:
                continue
            # An assignment from anything but a carried value or a mapping read.
            if "source_path=" in stripped and any(
                suspect in stripped
                for suspect in ("replace(", "rpartition", "rsplit(", "+ ", 'f"')
            ):
                offenders.append(f"{module.name}: {stripped}")

    assert not offenders, (
        "these look like they derive an authored path rather than carry one: "
        f"{offenders}"
    )

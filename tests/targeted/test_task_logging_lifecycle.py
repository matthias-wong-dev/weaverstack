"""The generic task log: where evidence goes, and what it is allowed to do.

Tested independently of load execution, because it is not a load feature. Weaver
has five top-level tasks and one evidence format, and a logger proven only
through the operation that first needed it would acquire that operation's shape
without anyone noticing.

Two separate claims live here and they are worth keeping apart:

``_.Log`` is a **declared Weaver document**, so it reaches the target through the
ordinary artefact lifecycle rather than through a path the logger knows.

The **writer** is tested against a folder location, with a `tmp_path` store and
no control plane anywhere near it — which is the point of taking the location
rather than resolving one.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from weaver.catalogue.builtin import LOG_FOLDER_ID, LOG_PATH
from weaver.catalogue.state import Catalogue
from weaver.catalogue.tables import FOLDER_DICTIONARY, REGISTRY
from weaver.declaration.metadata import FOLDER
from weaver.declaration.model import WeaverDocumentId
from weaver.errors import CommandError
from weaver.locations import Location
from weaver.resolution import LocalResolver
from weaver.store import LocalStore
from weaver.targets import ItemRef
from weaver.task_logging import (
    COMPLETE_STEP,
    DATE_PARTITION,
    PLAN_FILE,
    TASK_TYPES,
    log_folder,
    open_task_log,
)
from weaver.workspaces import LocalWorkspace

from factories import full_estate

LOG_IDENTITY = "Lakehouse/_weaver/Files/_.Log"


# --- the folder is an artefact, not a path ------------------------------------


def test_log_folder_is_a_normal_weaver_document(tmp_path):
    repository = full_estate(tmp_path)
    identity = WeaverDocumentId.parse(LOG_IDENTITY)

    document = repository.source_documents[identity]
    assert document.kind == FOLDER
    assert document.document.object_id.qualified == LOG_FOLDER_ID
    assert document.relative_path == LOG_PATH


def test_the_log_folder_reaches_the_catalogue_like_any_other_folder(tmp_path):
    repository = full_estate(tmp_path)
    catalogue = Catalogue.from_repository(repository)

    rows = catalogue.rows[WeaverDocumentId.parse(LOG_IDENTITY).item]
    assert {
        (row["schema_name"], row["object_name"], row["object_type"])
        for row in rows[REGISTRY.name]
    } >= {("Files/_", "Log", "folder")}
    assert {
        (row["schema_name"], row["object_name"])
        for row in rows[FOLDER_DICTIONARY.name]
    } == {("Files/_", "Log")}


def test_the_log_folder_is_resolved_through_its_declared_identity():
    resolver = LocalResolver(
        LocalWorkspace(workspace=".local", weaver_lakehouse="Weaver_LH")
    )

    assert log_folder(resolver, ItemRef("Weaver_LH")).value == (
        ".local/Weaver_LH/Files/_/Log"
    )


# --- the writer ---------------------------------------------------------------


@pytest.fixture
def folder(tmp_path) -> Location:
    return Location(str(tmp_path / "Files" / "_" / "Log"))


def fixed_clock(*moments):
    """A clock that answers each moment in turn, then repeats the last."""

    remaining = list(moments)

    def clock():
        return remaining.pop(0) if len(remaining) > 1 else remaining[0]

    return clock


def at(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)


def contents(location: Location) -> dict:
    return json.loads(LocalStore().read(location).decode("utf-8"))


def names(store: LocalStore, location: Location) -> list[str]:
    return sorted(entry.name for entry in store.list(location))


def test_task_logging_writes_beneath_the_declared_log_folder(folder):
    store = LocalStore()
    log = open_task_log(task_type="load", folder=folder, store=store)

    assert log.root.value.startswith(folder.value + "/")


def test_task_logging_partitions_by_utc_task_date(folder):
    log = open_task_log(
        task_type="load",
        folder=folder,
        store=LocalStore(),
        clock=fixed_clock(at("2026-08-03T09:15:22.123456")),
    )

    assert log.partition == f"{DATE_PARTITION}=2026-08-03"
    assert log.root.value == (
        f"{folder.value}/{DATE_PARTITION}=2026-08-03/"
        f"20260803T091522.123456Z_load_{log.task_id}"
    )


def test_a_task_stays_in_the_partition_it_started_in(folder):
    """A run is one thing; splitting it over midnight would make it two."""

    log = open_task_log(
        task_type="load",
        folder=folder,
        store=LocalStore(),
        clock=fixed_clock(
            at("2026-08-03T23:59:59.000000"), at("2026-08-04T00:00:31.000000")
        ),
    )
    step = log.write_step("load", {"node_id": "one"})

    assert f"{DATE_PARTITION}=2026-08-03" in step.value
    assert f"{DATE_PARTITION}=2026-08-04" not in step.value


def test_task_logging_writes_one_immutable_plan(folder):
    store = LocalStore()
    log = open_task_log(task_type="load", folder=folder, store=store)

    location = log.write_plan({"order": ["one", "two"], "mode": "execute"})

    assert location.value.endswith(f"/{PLAN_FILE}")
    assert contents(location) == {
        "order": ["one", "two"],
        "mode": "execute",
        "task_id": log.task_id,
        "task_type": "load",
    }
    assert names(store, log.root) == [PLAN_FILE]


def test_task_logging_writes_one_result_file_per_executed_step(folder):
    store = LocalStore()
    log = open_task_log(task_type="load", folder=folder, store=store)

    log.write_step("load", {"node_id": "one"})
    log.write_step("refresh", {"node_id": "two"})
    log.write_step("load", {"node_id": "three"})

    written = names(store, log.root)
    assert len(written) == 3
    assert sum("_load_" in name for name in written) == 2
    assert sum("_refresh_" in name for name in written) == 1


def test_a_step_file_names_its_broad_kind_and_carries_its_exact_identity(folder):
    log = open_task_log(task_type="load", folder=folder, store=LocalStore())

    location = log.write_step(
        "refresh",
        {"node_id": "refresh:Lakehouse/Raw_LH", "physical_target": "Lakehouse/Raw_LH"},
    )

    assert "_refresh_" in location.value.rsplit("/", 1)[-1]
    assert contents(location)["node_id"] == "refresh:Lakehouse/Raw_LH"
    assert contents(location)["step_type"] == "refresh"


def test_task_logging_embeds_messages_in_the_step_result(folder):
    """One file per step, never one per message."""

    store = LocalStore()
    log = open_task_log(task_type="load", folder=folder, store=store)

    location = log.write_step(
        "load",
        {
            "node_id": "one",
            "messages": [
                {"severity": "warning", "code": "primitive_rejects", "message": "two"},
                {"severity": "error", "code": "primitive_failure", "message": "three"},
            ],
        },
    )

    assert len(contents(location)["messages"]) == 2
    assert names(store, log.root) == [location.value.rsplit("/", 1)[-1]]


def test_task_logging_writes_one_completion_file(folder):
    store = LocalStore()
    log = open_task_log(task_type="load", folder=folder, store=store)
    log.write_step("load", {"node_id": "one"})

    location = log.write_completion({"final_status": "succeeded", "planned_steps": 1})

    assert location.value.endswith(f"_{COMPLETE_STEP}_{log.task_id}.json")
    assert contents(location)["final_status"] == "succeeded"
    assert sum(f"_{COMPLETE_STEP}_" in name for name in names(store, log.root)) == 1


def test_task_logging_leaves_no_completion_file_for_an_interrupted_task(folder):
    """The absence of the completion file is how an interruption is visible."""

    store = LocalStore()
    log = open_task_log(task_type="load", folder=folder, store=store)
    log.write_plan({"order": ["one", "two"]})
    log.write_step("load", {"node_id": "one"})
    # ... and the session dies here.

    written = names(store, log.root)
    assert PLAN_FILE in written
    assert not any(f"_{COMPLETE_STEP}_" in name for name in written)
    # What completed before the interruption is still identifiable from the plan
    # and the step files alone.
    assert len(written) == 2


def test_nothing_is_rewritten_after_it_is_written(folder):
    store = LocalStore()
    log = open_task_log(task_type="load", folder=folder, store=store)

    first = log.write_step("load", {"node_id": "one"})
    before = store.read(first)
    log.write_step("load", {"node_id": "one"})

    assert store.read(first) == before
    assert len(names(store, log.root)) == 2


def test_task_log_layout_supports_build_load_wipe_mirror_and_test(folder):
    store = LocalStore()

    roots = {
        task_type: open_task_log(
            task_type=task_type, folder=folder, store=store
        ).root.value
        for task_type in TASK_TYPES
    }

    assert set(roots) == {"wipe", "mirror", "build", "load", "test"}
    for task_type, root in roots.items():
        assert f"_{task_type}_" in root.rsplit("/", 1)[-1]


def test_a_plan_need_not_be_a_dag(folder):
    """The format is generic: a build's plan is stages, and that is a plan too."""

    log = open_task_log(task_type="build", folder=folder, store=LocalStore())

    location = log.write_plan(
        {"stages": [{"name": "prune"}, {"name": "schema"}, {"name": "build"}]}
    )

    assert [stage["name"] for stage in contents(location)["stages"]] == [
        "prune",
        "schema",
        "build",
    ]


def test_an_unknown_task_type_is_refused(folder):
    with pytest.raises(CommandError, match="is not a Weaver task type"):
        open_task_log(task_type="reticulate", folder=folder, store=LocalStore())

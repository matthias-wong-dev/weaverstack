"""Numbering the assembled plan and compiling its physical DAG."""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.build_bundle.dependencies import (
    DECERTIFIED,
    PHYSICAL_COMPLETE,
    PREPARED,
    object_key,
)
from weaver.build_bundle.models import BuildBatch, InstallAction
from weaver.build_bundle.stages import (
    BUILD,
    CATALOGUE,
    PRUNE,
    SHORTCUT,
    TEMPORARY_VIEWS,
    PlannedStage,
    merge_layer_stages,
)
from weaver.build_bundle.stages import enumerate_stages as _enumerate
from weaver.build_bundle.targets import BoundTarget
from weaver.errors import BuildError

TARGETS = (
    BoundTarget("target", "lakehouse", "Sales"),
    BoundTarget("catalogue", "warehouse", "Weaver"),
)


def enumerate_stages(stages):
    sequences, payloads, changes, _required = _enumerate(
        stages, targets=TARGETS, completion_target_id="catalogue"
    )
    return sequences, payloads, changes


def compiled(stages):
    sequences, _payloads, _changes, required = _enumerate(
        stages, targets=TARGETS, completion_target_id="catalogue"
    )
    actions = {
        action.id: action
        for sequence in sequences
        for batch in sequence.batches
        for action in batch.actions
    }
    return actions, required


def _action(name, payload=None):
    return InstallAction(
        id=name,
        kind="build_table",
        resource_node_id=None,
        executor="spark_sql",
        payload=payload,
        payload_sha256=None if payload is None else "0" * 64,
    )


def _stage(phase, *, index=0, slug="things", batch="one", payloads=None, actions=None):
    return PlannedStage(
        phase=phase,
        index=index,
        slug=slug,
        description=f"{phase} work",
        payloads=payloads or {},
        batches=(
            BuildBatch(
                id=batch,
                target_id="target",
                actions=tuple(actions or (_action(f"{batch}-action"),)),
            ),
        ),
    )


@weaver_test()
def test_stages_are_numbered_consecutively_from_one():
    sequences, _payloads, _changes = enumerate_stages(
        [_stage(PRUNE), _stage(SHORTCUT), _stage(BUILD)]
    )

    assert [sequence.number for sequence in sequences] == [1, 2, 3]


@weaver_test()
def test_an_empty_stage_takes_no_number_and_leaves_no_gap():
    empty = PlannedStage(phase=SHORTCUT, description="nothing to shortcut", batches=())

    sequences, _payloads, _changes = enumerate_stages(
        [_stage(PRUNE), empty, _stage(BUILD)]
    )

    assert [sequence.number for sequence in sequences] == [1, 2]
    assert [sequence.description for sequence in sequences] == [
        "prune work",
        "build work",
    ]


@weaver_test()
def test_payload_paths_and_batch_ids_gain_the_number_they_were_given():
    sequences, payloads, _changes = enumerate_stages(
        [
            _stage(PRUNE),
            _stage(
                BUILD,
                slug="build-objects",
                payloads={"customer.spark.sql": b"CREATE"},
                actions=(_action("object-customer", payload="customer.spark.sql"),),
            ),
        ]
    )

    build = sequences[1]
    assert build.batches[0].id == "002-one"
    assert build.batches[-1].id == "completion:build"
    assert build.batches[0].actions[0].payload == (
        "payload/002-build-objects/customer.spark.sql"
    )
    assert payloads == {"payload/002-build-objects/customer.spark.sql": b"CREATE"}


@weaver_test()
def test_one_layers_same_phase_stages_become_one_barrier_with_a_batch_each():
    merged = merge_layer_stages(
        [
            _stage(BUILD, slug="build-objects", batch="raw"),
            _stage(PRUNE, slug="item-prune", batch="raw"),
            _stage(BUILD, slug="build-objects", batch="curated"),
            _stage(PRUNE, slug="item-prune", batch="curated"),
        ]
    )

    assert [stage.phase for stage in merged] == [PRUNE, BUILD]
    assert [[batch.id for batch in stage.batches] for stage in merged] == [
        ["raw", "curated"],
        ["raw", "curated"],
    ]


@weaver_test()
def test_dependency_layers_within_a_phase_stay_separate_barriers():
    merged = merge_layer_stages(
        [
            _stage(BUILD, index=1, batch="raw-second"),
            _stage(BUILD, index=0, batch="raw-first"),
        ]
    )

    assert [stage.index for stage in merged] == [0, 1]
    assert [stage.batches[0].id for stage in merged] == ["raw-first", "raw-second"]


@weaver_test()
def test_a_payload_key_that_is_not_a_bare_filename_is_refused():
    with pytest.raises(BuildError, match="bare filename"):
        PlannedStage(
            phase=BUILD,
            description="build work",
            batches=(),
            payloads={"payload/010-build/customer.sql": b""},
        )


@weaver_test()
def test_an_action_naming_a_payload_its_stage_did_not_supply_is_refused():
    stage = _stage(BUILD, actions=(_action("object", payload="missing.spark.sql"),))

    with pytest.raises(BuildError, match="which its stage did not supply"):
        enumerate_stages([stage])


@weaver_test()
def test_merged_stages_must_agree_about_their_payload_directory():
    with pytest.raises(BuildError, match="disagree about their payload directory"):
        merge_layer_stages(
            [
                _stage(BUILD, slug="build-objects", batch="raw"),
                _stage(BUILD, slug="something-else", batch="curated"),
            ]
        )


def _keyed(phase, name, *, target="target", executor="spark_sql", **keys):
    return PlannedStage(
        phase=phase,
        description=f"{name} work",
        batches=(
            BuildBatch(
                id=name,
                target_id=target,
                actions=(
                    InstallAction(
                        id=name,
                        kind="build_table",
                        resource_node_id=None,
                        executor=executor,
                        payload=None,
                        payload_sha256=None,
                    ),
                ),
            ),
        ),
        **{key: {name: tuple(value)} for key, value in keys.items()},
    )


@weaver_test()
def test_a_required_key_becomes_a_success_edge_to_every_provider():
    actions, _ = compiled(
        [
            _keyed(BUILD, "a", provides=[object_key("A")]),
            _keyed(BUILD, "b", provides=[object_key("A")]),
            _keyed(BUILD, "consumer", requires=[object_key("A"), object_key("Absent")]),
        ]
    )

    # A key nothing in the plan provides is already satisfied by the target.
    assert actions["consumer"].depends_on == ("a", "b")
    assert actions["a"].depends_on == () and actions["b"].depends_on == ()


@weaver_test()
def test_a_followed_key_orders_after_a_known_outcome_only():
    actions, _ = compiled(
        [
            _keyed(BUILD, "a", provides=[object_key("A")]),
            _keyed(BUILD, "refresh", follows=[object_key("A")]),
        ]
    )

    assert actions["refresh"].settle_after == ("a",)
    assert actions["refresh"].depends_on == ()


@weaver_test()
def test_independent_branches_share_no_edge():
    actions, _ = compiled(
        [
            _keyed(BUILD, "a", provides=[object_key("A")]),
            _keyed(BUILD, "b", provides=[object_key("B")]),
            _keyed(BUILD, "a2", requires=[object_key("A")]),
            _keyed(BUILD, "b2", requires=[object_key("B")]),
        ]
    )

    assert actions["a2"].depends_on == ("a",)
    assert actions["b2"].depends_on == ("b",)


@weaver_test()
def test_physical_roots_wait_for_catalogue_preparation_and_publication_for_all():
    actions, required = compiled(
        [
            _keyed(
                CATALOGUE,
                "decertify",
                target="catalogue",
                executor="tsql_batch",
                provides=[DECERTIFIED, PREPARED],
            ),
            _keyed(
                CATALOGUE,
                "reset",
                target="catalogue",
                executor="runtime_state",
                requires=[DECERTIFIED],
                provides=[PREPARED],
            ),
            _keyed(BUILD, "a", provides=[object_key("A")]),
            _keyed(BUILD, "a2", requires=[object_key("A")]),
            _keyed(BUILD, "b"),
            _keyed(
                CATALOGUE,
                "publish",
                target="catalogue",
                executor="tsql_batch",
                requires=[PHYSICAL_COMPLETE],
            ),
        ]
    )

    assert actions["decertify"].depends_on == ()
    assert actions["reset"].depends_on == ("decertify",)
    assert actions["a"].depends_on == ("decertify", "reset")
    assert actions["a2"].depends_on == ("a",)
    # The gate needs only the success sinks; their ancestors come with them.
    assert actions["complete-physical-work"].depends_on == ("a2", "b")
    assert actions["publish"].depends_on == ("complete-physical-work",)
    assert required == ("complete-build",)
    assert actions["complete-build"].depends_on == ("publish",)


@weaver_test()
def test_publication_without_physical_work_still_follows_preparation():
    actions, _ = compiled(
        [
            _keyed(
                CATALOGUE,
                "decertify",
                target="catalogue",
                executor="tsql_batch",
                provides=[DECERTIFIED, PREPARED],
            ),
            _keyed(
                CATALOGUE,
                "reset",
                target="catalogue",
                executor="runtime_state",
                requires=[DECERTIFIED],
                provides=[PREPARED],
            ),
            _keyed(
                CATALOGUE,
                "publish",
                target="catalogue",
                executor="tsql_batch",
                requires=[PHYSICAL_COMPLETE],
            ),
        ]
    )

    assert actions["complete-physical-work"].depends_on == ("reset",)
    assert actions["publish"].depends_on == ("complete-physical-work",)


@weaver_test()
def test_resources_name_the_capability_and_setup_tables_exclude_each_other():
    import json

    def table(name, setup):
        content = json.dumps({"setup": setup}).encode()
        return PlannedStage(
            phase=BUILD,
            description="tables",
            payloads={f"{name}.spark-table.json": content},
            batches=(
                BuildBatch(
                    id=name,
                    target_id="target",
                    actions=(
                        InstallAction(
                            id=name,
                            kind="build_table",
                            resource_node_id=None,
                            executor="spark_table",
                            payload=f"{name}.spark-table.json",
                            payload_sha256="0" * 64,
                        ),
                    ),
                ),
            ),
        )

    actions, _ = compiled(
        [
            table("plain", []),
            table("staged", ["CREATE TEMPORARY VIEW staged AS SELECT 1"]),
            _keyed(CATALOGUE, "publish", target="catalogue", executor="tsql_batch"),
        ]
    )

    assert actions["plain"].resources == ("spark",)
    assert actions["plain"].exclusions == ()
    assert actions["staged"].exclusions == (TEMPORARY_VIEWS,)
    assert actions["publish"].resources == ("warehouse:Weaver",)

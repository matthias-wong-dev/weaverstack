"""Numbering the assembled plan — the one place a sequence number is chosen."""

from __future__ import annotations

import pytest

from weaver.build_bundle.models import BuildBatch, InstallAction
from weaver.build_bundle.stages import (
    ALIAS,
    BUILD,
    PRUNE,
    PlannedStage,
    enumerate_stages,
    merge_layer_stages,
)
from weaver.errors import BuildError


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


def test_stages_are_numbered_consecutively_from_one():
    sequences, _payloads, _changes = enumerate_stages(
        [_stage(PRUNE), _stage(ALIAS), _stage(BUILD)]
    )

    assert [sequence.number for sequence in sequences] == [1, 2, 3]


def test_an_empty_stage_takes_no_number_and_leaves_no_gap():
    empty = PlannedStage(phase=ALIAS, description="nothing to alias", batches=())

    sequences, _payloads, _changes = enumerate_stages(
        [_stage(PRUNE), empty, _stage(BUILD)]
    )

    assert [sequence.number for sequence in sequences] == [1, 2]
    assert [sequence.description for sequence in sequences] == [
        "prune work",
        "build work",
    ]


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
    assert build.batches[0].actions[0].payload == (
        "payload/002-build-objects/customer.spark.sql"
    )
    assert payloads == {"payload/002-build-objects/customer.spark.sql": b"CREATE"}


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


def test_dependency_layers_within_a_phase_stay_separate_barriers():
    merged = merge_layer_stages(
        [
            _stage(BUILD, index=1, batch="raw-second"),
            _stage(BUILD, index=0, batch="raw-first"),
        ]
    )

    assert [stage.index for stage in merged] == [0, 1]
    assert [stage.batches[0].id for stage in merged] == ["raw-first", "raw-second"]


def test_a_payload_key_that_is_not_a_bare_filename_is_refused():
    with pytest.raises(BuildError, match="bare filename"):
        PlannedStage(
            phase=BUILD,
            description="build work",
            batches=(),
            payloads={"payload/010-build/customer.sql": b""},
        )


def test_an_action_naming_a_payload_its_stage_did_not_supply_is_refused():
    stage = _stage(BUILD, actions=(_action("object", payload="missing.spark.sql"),))

    with pytest.raises(BuildError, match="which its stage did not supply"):
        enumerate_stages([stage])


def test_merged_stages_must_agree_about_their_payload_directory():
    with pytest.raises(BuildError, match="disagree about their payload directory"):
        merge_layer_stages(
            [
                _stage(BUILD, slug="build-objects", batch="raw"),
                _stage(BUILD, slug="something-else", batch="curated"),
            ]
        )

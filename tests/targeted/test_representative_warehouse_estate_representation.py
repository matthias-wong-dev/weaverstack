"""Representative Warehouse estates keep authored work and graph evidence aligned."""

from __future__ import annotations

import re

import pytest
from factories import FixtureCatalogue, item_bindings, target_inventory
from support.representative_warehouse_estate import (
    RepresentativeWarehouseSpec,
    make_representative_oracle,
    parse_representative_estate,
    qualify_representative_estate,
    write_representative_estate,
)
from support.weaver_test import weaver_test
from support.workspaces import WORKSPACE

from weaver.build_bundle import (
    WarehouseBinding,
    effective_item_bindings,
    generate_item_build_bundle,
    load_bundle,
)
from weaver.etl import ROLE_ASSUMPTION, ROLE_TEST
from weaver.locations import Location
from weaver.store import FilesystemStore
from weaver.targets import ItemRef


@weaver_test()
def test_one_motif_has_the_exact_declaration_mix_and_real_sql_shapes():
    oracle = make_representative_oracle(RepresentativeWarehouseSpec(motifs=1, seed=17))

    assert oracle.kind_counts == {
        "table": 14,
        "view": 6,
        "test": 3,
        "assumption": 2,
    }
    assert {node.sql_shape for node in oracle.nodes} >= {
        "source_rows",
        "materialised_join",
        "materialised_aggregate",
        "filter",
        "projection",
        "calculation",
        "join",
        "union",
        "grouped_aggregate",
        "window",
        "reconciliation",
        "assumption",
    }
    assert oracle.statistics["maximum_depth"] >= 7
    assert oracle.statistics["maximum_fan_in"] == 3
    assert oracle.statistics["maximum_fan_out"] >= 3
    assert len(oracle.roots) == 2
    assert set(oracle.leaves) == {
        node.identity for node in oracle.nodes if node.kind in {"test", "assumption"}
    }


@weaver_test()
def test_seed_changes_source_data_and_repeats_exactly(tmp_path):
    first = make_representative_oracle(
        RepresentativeWarehouseSpec.from_declarations(50, seed=101)
    )
    repeated = make_representative_oracle(
        RepresentativeWarehouseSpec.from_declarations(50, seed=101)
    )
    another = make_representative_oracle(
        RepresentativeWarehouseSpec.from_declarations(50, seed=102)
    )

    assert first == repeated
    assert first.source_rows == repeated.source_rows
    assert first.source_rows != another.source_rows

    write_representative_estate(tmp_path / "first", first)
    write_representative_estate(tmp_path / "repeated", repeated)
    assert {
        path.relative_to(tmp_path / "first"): path.read_bytes()
        for path in (tmp_path / "first").rglob("*")
        if path.is_file()
    } == {
        path.relative_to(tmp_path / "repeated"): path.read_bytes()
        for path in (tmp_path / "repeated").rglob("*")
        if path.is_file()
    }


@weaver_test()
def test_two_motifs_cross_an_item_boundary_through_consumed_shortcut_sql(tmp_path):
    oracle = make_representative_oracle(
        RepresentativeWarehouseSpec.from_declarations(50)
    )
    write_representative_estate(tmp_path, oracle)
    repository = parse_representative_estate(tmp_path)

    assert len(oracle.shortcuts) == 1
    shortcut = oracle.shortcuts[0]
    assert shortcut.kind == "view"
    assert shortcut.target_type == "logical"
    assert shortcut.source_item != shortcut.destination_item
    assert shortcut.consumers
    assert shortcut.consumed is True

    consumer = oracle.by_identity[shortcut.consumers[0]]
    source = (tmp_path / consumer.relative_path).read_text(encoding="utf-8")
    assert shortcut.reference_sql in source

    parsed_pairs = {
        (str(pair.source), str(pair.destination))
        for pair in repository.logical_shortcuts
        if str(pair.destination) in oracle.graph_nodes
    }
    assert parsed_pairs == set(oracle.shortcut_edges)


@weaver_test()
def test_fifty_declarations_produce_a_complete_ordered_cold_build_bundle(tmp_path):
    spec = RepresentativeWarehouseSpec.from_declarations(50)
    oracle = make_representative_oracle(spec)
    source = tmp_path / "source"
    write_representative_estate(source, oracle)
    repository = parse_representative_estate(source)
    bindings = effective_item_bindings(
        item_bindings(
            *(
                (
                    f"Warehouse/Representative{motif:03d}",
                    f"Representative{motif:03d}_WH",
                )
                for motif in range(spec.motifs)
            )
        ),
        control_item=ItemRef("Weaver"),
        workspace_name=WORKSPACE,
    )
    inventories = {}
    for binding in bindings.entries:
        target = binding.to_bound_target()
        inventories[binding.item] = target_inventory(
            target_id=target.id,
            kind=target.kind,
            target_name=target.name,
        )
    output = Location(str(tmp_path / "bundle"))
    store = FilesystemStore()
    bundle = generate_item_build_bundle(
        repository,
        bindings=bindings,
        output=output,
        store=store,
        target_inventories=inventories,
        catalogue=FixtureCatalogue.from_registry_rows(),
        catalogue_binding=WarehouseBinding(ItemRef("Weaver"), workspace_name=WORKSPACE),
    )
    loaded = load_bundle(output, store=store)
    actions = tuple(bundle.plan.actions())
    action_resources = {
        kind: {
            action.resource_node_id
            for _sequence, _batch, action in actions
            if action.kind == kind and action.resource_node_id is not None
        }
        for kind in ("build_table", "build_view", "build_procedure")
    }
    representative_items = {
        f"Warehouse/Representative{motif:03d}" for motif in range(spec.motifs)
    }
    expected_tables = {node.identity for node in oracle.nodes if node.kind == "table"}
    expected_views = {node.identity for node in oracle.nodes if node.kind == "view"}
    expected_programmables = {
        str(programmable.identity)
        for programmable in repository.programmables.values()
        if str(programmable.identity.item) in representative_items
    }
    expected_validations = {
        str(programmable.identity)
        for programmable in repository.programmables.values()
        if str(programmable.identity.item) in representative_items
        and programmable.role in {ROLE_TEST, ROLE_ASSUMPTION}
    }

    assert bundle.plan.omitted_nodes == ()
    assert loaded.plan.to_mapping() == bundle.plan.to_mapping()
    assert expected_tables <= action_resources["build_table"]
    assert expected_views <= action_resources["build_view"]
    assert expected_programmables <= action_resources["build_procedure"]
    assert len(expected_validations) == 10
    assert expected_validations <= action_resources["build_procedure"]
    assert {shortcut.destination for shortcut in oracle.shortcuts} <= {
        str(identity) for identity in bundle.plan.selection.selected_for_build
    }

    sequence_by_resource = {
        action.resource_node_id: sequence.number
        for sequence, _batch, action in actions
        if action.resource_node_id is not None
    }
    sequence_by_action = {
        action.id: sequence.number for sequence, _batch, action in actions
    }
    shortcut = oracle.shortcuts[0]
    shortcut_action = "shortcuts-" + shortcut.destination_item.replace("/", "--")
    assert (
        sequence_by_resource[shortcut.source]
        < sequence_by_action[shortcut_action]
        < sequence_by_resource[shortcut.consumers[0]]
    )


@weaver_test()
def test_every_non_root_body_physically_reads_its_oracle_dependencies(tmp_path):
    oracle = make_representative_oracle(
        RepresentativeWarehouseSpec.from_declarations(50)
    )
    write_representative_estate(tmp_path, oracle)

    for node in oracle.nodes:
        source = (tmp_path / node.relative_path).read_text(encoding="utf-8")
        body = source.split("*/", 1)[1]
        if node.root_source:
            assert "Representative root source rows." in source
            assert "values" in body.casefold()
            continue

        assert node.references
        assert re.search(r"\b(from|join)\b", body, flags=re.IGNORECASE)
        for reference in node.reference_sql:
            assert reference in body, f"{node.identity} does not read {reference}"


@pytest.mark.parametrize(
    ("declarations", "expected", "components", "main_declarations"),
    [
        (50, {"table": 28, "view": 12, "test": 6, "assumption": 4}, 1, 25),
        (250, {"table": 140, "view": 60, "test": 30, "assumption": 20}, 2, 225),
        (
            1000,
            {"table": 560, "view": 240, "test": 120, "assumption": 80},
            8,
            975,
        ),
    ],
)
@weaver_test()
def test_required_scales_match_the_independent_oracle_exactly(
    tmp_path, declarations, expected, components, main_declarations
):
    result = qualify_representative_estate(
        tmp_path,
        RepresentativeWarehouseSpec.from_declarations(declarations),
    )

    assert result["profile"] == "representative_warehouse_build"
    assert result["declarations"] == {"total": declarations, "by_kind": expected}
    assert result["items"] == {
        "engine": {"warehouse": declarations},
        "count": 2,
        "declarations_by_item": {
            "Warehouse/Representative000": 25,
            "Warehouse/Representative001": main_declarations,
        },
    }
    assert result["validations"] == {
        "total": expected["test"] + expected["assumption"],
        "test": expected["test"],
        "assumption": expected["assumption"],
        "executed": 0,
    }
    assert result["shortcuts"]["count"] == 1
    assert result["shortcuts"]["consumed"] == 1
    assert {
        (shortcut["source_item"], shortcut["destination_item"])
        for shortcut in result["shortcuts"]["census"]
    } == {("Warehouse/Representative000", "Warehouse/Representative001")}
    assert result["graph"]["connected_components"] == components
    assert result["graph"]["maximum_depth"] >= 7
    assert result["graph"]["maximum_fan_in"] == 3
    assert result["graph"]["maximum_fan_out"] >= 3
    assert result["oracle_matches_repository"] == {
        "declaration_identities": True,
        "declaration_kinds": True,
        "ordinary_edges": True,
        "shortcut_edges": True,
        "roots": True,
        "leaves": True,
        "depth": True,
        "fan_in": True,
        "fan_out": True,
        "connected_components": True,
        "item_engine_distribution": True,
        "validation_counts": True,
        "shortcut_census": True,
        "physical_references": True,
        "non_root_queries": True,
    }

"""Source guard keeps the fixed PBIP's local literal M tables separate from SQL."""

import copy
from pathlib import Path

import pytest
from support.weaver_test import weaver_test
from test_semantic_fixture_source_boundary import PARTS, contract

from weaver.semantic_models import TmdlDefinition
from weaver.semantic_models.definition import encode_parts

FIXTURE = (
    Path(__file__).parent
    / "fixtures/semantic_model/Probe/Probe.SemanticModel/definition/tables"
)


@weaver_test()
def test_configured_guard_retains_literal_pbip_imports_and_sql_carrier():
    model, source = contract()
    parts = copy.deepcopy(PARTS)
    parts["definition/model.tmdl"] += b"\nref table Sales\nref table Product\n"
    for name in ("Sales", "Product"):
        parts[f"definition/tables/{name}.tmdl"] = (
            FIXTURE / f"{name}.tmdl"
        ).read_bytes()
    source.guard_definition(encode_parts(parts))
    observed = copy.deepcopy(model.observed)
    for table in TmdlDefinition(parts).model.tables:
        if table.name == "CatalogueObjects":
            continue
        observed["model"]["tables"].append(
            {
                "name": table.name,
                "partitions": [
                    {
                        "name": p.name,
                        "mode": p.mode,
                        "source": {"type": "m", "expression": p.source},
                    }
                    for p in table.partitions
                ],
            }
        )
    model.observed = observed
    source.verify("pbip-local-literals")
    assert source.evidence[-1]["stage"] == "pbip-local-literals"
    removed = {
        name: value
        for name, value in parts.items()
        if name != "definition/tables/CatalogueObjects.tmdl"
    }
    removed["definition/model.tmdl"] = (
        b"model Model\n    culture: en-US\n\nref table Sales\nref table Product\n"
    )
    with pytest.raises(AssertionError, match="source was removed"):
        source.guard_definition(encode_parts(removed))


@weaver_test()
@pytest.mark.parametrize(
    "expression",
    [
        '#table({"x"}, {{Sql.Database("other", "database")}})',
        'let Source = Web.Contents("https://other.example") in Source',
        'let Source = #table(type table [ProductId = Int64.Type, ProductName = text], {{10, "Cake"}, {20, "Coffee"}}) in Table.Combine({Source, Other})',
    ],
)
def test_unknown_import_m_refused_even_beside_a_retained_sql_carrier(expression):
    _, source = contract()
    parts = copy.deepcopy(PARTS)
    parts["definition/model.tmdl"] += b"\nref table Other\n"
    parts["definition/tables/Other.tmdl"] = (
        "table Other\n    partition Other = m\n        mode: import\n        source =\n"
        + "\n".join("            " + line for line in expression.splitlines())
        + "\n"
    ).encode()
    with pytest.raises(AssertionError, match="source"):
        source.guard_definition(encode_parts(parts))

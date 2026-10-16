"""The Catalogue Dashboard assesses Health as `weaver health` does.

The Dashboard cannot run Python, so its renderer restates the Load, Tests and
Build rules. Each estate here is assessed twice over the same catalogue rows: by
:func:`weaver.health.assess`, and by the renderer under Node over the payload
the model's measures carry. The findings must agree on area, code, severity
and object.

What the Dashboard cannot observe stays out of the comparison: a physical
inventory, references the graph does not resolve, and installation identity.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest
from support.dashboard_payload import page_payload
from support.weaver_test import weaver_test
from test_health_representation import NOW, RAW, REPORTING, YESTERDAY, _Estate, at

from weaver.catalogue.claims import catalogue_columns
from weaver.catalogue.graph_rows import graph_rows
from weaver.catalogue.state import Catalogue
from weaver.health import (
    AMBIGUOUS_INSTALLATION,
    CERTIFIED_MISSING,
    DEPENDENCY_UNRESOLVED,
    INSTALLATION_INCONSISTENT,
)

#: Build checks that need the physical estate or the authored references.
UNOBSERVED = {
    CERTIFIED_MISSING,
    DEPENDENCY_UNRESOLVED,
    AMBIGUOUS_INSTALLATION,
    INSTALLATION_INCONSISTENT,
}

RENDERER = Path(__file__).parents[1] / "js" / "renderer.js"

ASSESS = """
const D = require(process.argv[1]).load();
let text = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => { text += chunk; });
process.stdin.on("end", () => {
  const parsed = D.parsePayload(text);
  const m = D.buildModel(parsed.tables, parsed.problems);
  const health = D.assessHealth(m, { asOf: Number(process.argv[2]) });
  const found = health.all.map((f) => [f.area, f.code, f.severity, f.node ? f.node.id : [f.item, f.schema || "", f.object || ""].join("|")]);
  process.stdout.write(JSON.stringify({ findings: found, problems: m.problems }));
});
"""


def _node() -> str | None:
    return os.environ.get("WEAVER_NODE") or shutil.which("node")


def estates():
    shortcut_summary = f"{REPORTING}/Sales.Summary"
    return {
        "green": _Estate()
        .table(f"{RAW}/Tables/Sales.Order", loaded=at(1), moved=at(1))
        .validation(f"{RAW}/Sales.Integrity", result="succeeded", ran=at(0.5))
        .reads(f"{RAW}/Sales.Integrity", "Sales.Order"),
        "pending and missing": _Estate()
        .table(f"{RAW}/Tables/Sales.Order")
        .table(f"{RAW}/Tables/Sales.Line", result="pending"),
        "red outcomes": _Estate()
        .table(f"{RAW}/Tables/Sales.A", loaded=at(1), result="failed")
        .table(f"{RAW}/Tables/Sales.B", loaded=at(1), result="error")
        .table(f"{RAW}/Tables/Sales.C", loaded=at(1), result="blocked")
        .table(f"{RAW}/Tables/Sales.D", loaded=at(1), result="rejected"),
        "old success": _Estate().table(
            f"{RAW}/Tables/Sales.Order", loaded=at(30), moved=at(30)
        ),
        "behind an ancestor": _Estate()
        .table(f"{RAW}/Tables/Sales.A", loaded=at(1), moved=at(1))
        .table(f"{RAW}/Tables/Sales.B", loaded=at(5), moved=at(5))
        .table(f"{RAW}/Tables/Sales.C", loaded=at(6), moved=at(6))
        .reads(f"{RAW}/Tables/Sales.B", "Sales.A")
        .reads(f"{RAW}/Tables/Sales.C", "Sales.B"),
        "through a view": _Estate()
        .table(f"{RAW}/Tables/Sales.A", loaded=at(4), moved=at(4))
        .view(f"{RAW}/Tables/Sales.Live", loaded=at(3))
        .table(f"{RAW}/Tables/Sales.B", loaded=at(5), moved=at(5))
        .reads(f"{RAW}/Tables/Sales.Live", "Sales.A")
        .reads(f"{RAW}/Tables/Sales.B", "Sales.Live"),
        "through a shortcut": _Estate()
        .table(f"{RAW}/Tables/Sales.Order", loaded=at(1), moved=at(1))
        .shortcut(f"{REPORTING}/Sales.Order", f"{RAW}/Tables/Sales.Order")
        .table(shortcut_summary, loaded=at(5), moved=at(5))
        .reads(shortcut_summary, "Sales.Order"),
        "unsettled ancestor": _Estate()
        .table(f"{RAW}/Tables/Sales.A", result="pending")
        .table(f"{RAW}/Tables/Sales.B", loaded=at(1), moved=at(1))
        .table(f"{RAW}/Tables/Sales.C", loaded=at(1), result="blocked")
        .table(f"{RAW}/Tables/Sales.D", loaded=at(2), moved=at(2))
        .reads(f"{RAW}/Tables/Sales.B", "Sales.A")
        .reads(f"{RAW}/Tables/Sales.D", "Sales.C"),
        "static": _Estate()
        .table(
            f"{RAW}/Tables/Sales.Code", loaded=at(2000), moved=at(2000), is_static=True
        )
        .table(f"{RAW}/Tables/Sales.Fresh", loaded=at(1), moved=at(1))
        .table(f"{RAW}/Tables/Sales.Fixed", loaded=at(9), moved=at(9), is_static=True)
        .reads(f"{RAW}/Tables/Sales.Fixed", "Sales.Fresh"),
        "validations": _Estate()
        .table(f"{RAW}/Tables/Sales.A", loaded=at(6), moved=at(6))
        .table(f"{RAW}/Tables/Sales.B", loaded=at(1), moved=at(1))
        .table(f"{RAW}/Tables/Sales.Built", result="pending", built=at(1))
        .table(f"{RAW}/Tables/Sales.Kept", loaded=at(4), moved=at(4), built=at(5))
        .reads(f"{RAW}/Tables/Sales.B", "Sales.A")
        .validation(f"{RAW}/Sales.Moved", result="succeeded", ran=at(3))
        .reads(f"{RAW}/Sales.Moved", "Sales.B")
        .validation(f"{RAW}/Sales.Rebuilt", result="succeeded", ran=at(3))
        .reads(f"{RAW}/Sales.Rebuilt", "Sales.Built")
        .validation(f"{RAW}/Sales.Steady", result="succeeded", ran=at(3))
        .reads(f"{RAW}/Sales.Steady", "Sales.Kept")
        .validation(f"{RAW}/Sales.Failing", result="failed", ran=at(1), failure_count=4)
        .validation(f"{RAW}/Sales.Erroring", result="error", ran=at(1))
        .validation(f"{RAW}/Sales.Unrun")
        .validation(
            f"{RAW}/Sales.Assumed", kind="Assumption", result="pending", ran=at(1)
        ),
        "build": _Estate()
        .table(f"{RAW}/Tables/Sales.Order", loaded=at(1), moved=at(1))
        .declared_not_installed(f"{RAW}/Tables/Sales.Ghost")
        .validation(f"{RAW}/Sales.Absent", installed=False),
        "mirrored": _Estate()
        .table(f"{REPORTING}/Sales.Order", loaded=at(1), moved=at(1))
        .mirrors(f"{REPORTING}/Sales.Order"),
    }


def _dashboard_rows(catalogue: Catalogue) -> dict[str, list[dict]]:
    """Every item's rows by public table name, with the graph Build projects."""

    rows: dict[str, list[dict]] = {}
    for item, tables in catalogue.rows.items():
        merged = {**tables, **graph_rows(Catalogue({item: tables}), item)}
        for name, found in merged.items():
            rows.setdefault(name, []).extend(found)
    return rows


def _identity(dag, object_id: str) -> str:
    node = dag.by_id[object_id]
    schema, name = catalogue_columns(node.identity)
    return f"{node.item}|{schema or ''}|{name or ''}"


@pytest.mark.parametrize("name", list(estates()))
@weaver_test()
def test_the_dashboard_finds_what_weaver_health_finds(name):
    node = _node()
    if node is None:
        pytest.skip("Node.js is not on PATH; set WEAVER_NODE to run the renderer")
    estate = estates()[name]
    catalogue = estate.catalogue()
    report = estate.report()
    dag = catalogue.dag()

    expected = set()
    for section in report.sections:
        for finding in section.findings:
            if finding.code in UNOBSERVED:
                continue
            identity = (
                _identity(dag, finding.object_id)
                if finding.object_id in dag.by_id
                else _declared(finding.object_id)
            )
            expected.add((finding.area, finding.code, finding.severity, identity))

    payload = page_payload(_dashboard_rows(catalogue), "health", at=NOW)
    as_of = round(YESTERDAY.timestamp() * 1000)
    result = subprocess.run(
        [node, "-e", ASSESS, str(RENDERER), str(as_of)],
        input=payload,
        capture_output=True,
        # The payload's separators are private-use characters, which a
        # Windows code page cannot encode.
        encoding="utf-8",
        timeout=60,
        check=True,
    )
    seen = json.loads(result.stdout)

    assert seen["problems"] == []
    assert {tuple(f) for f in seen["findings"]} == expected
    # A static object is neither overdue nor behind, so that estate is green too.
    assert bool(expected) is (name not in ("green", "static"))


def _declared(object_id: str) -> str:
    """A declared object Registry does not certify has no graph node."""

    from weaver.declaration.model import WeaverDocumentId

    identity = WeaverDocumentId.parse(object_id)
    schema, name = catalogue_columns(identity)
    return f"{identity.item}|{schema}|{name}"

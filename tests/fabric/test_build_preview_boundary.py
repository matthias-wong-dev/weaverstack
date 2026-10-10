"""Public desktop preview reads Fabric without executing its proposed mutations."""

import hashlib
import json
import os
from pathlib import Path

from support.build_preview_guards import guard_preview
from support.weaver_test import register_session, weaver_test
from test_item_repository_declaration import _schema, _warehouse_table, _write

import weaver
from weaver.build_bundle import load_bundle
from weaver.catalogue.connection import catalogue_connection
from weaver.catalogue.state import read_target_occupancy
from weaver.locations import Location
from weaver.sessions import ConsoleSession
from weaver.store import FilesystemStore
from weaver.targets import ItemRef, WarehouseTarget
from weaver.workspaces import Workspace


def warehouse_snapshot(session, workspace, target):
    def query(statement):
        return list(session.query_tsql(statement, target=target, workspace=workspace))

    shape = query(
        "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS"
    )
    rows = {}
    for schema, table in sorted(
        {
            (r["TABLE_SCHEMA"], r["TABLE_NAME"])
            for r in shape
            if r["TABLE_SCHEMA"] == "_"
        }
    ):
        quoted = "[" + table.replace("]", "]]") + "]"
        rows[table] = sorted(
            query(f"SELECT * FROM [_].{quoted}"),
            key=lambda r: json.dumps(r, default=str, sort_keys=True),
        )
    inventory = query(
        "SELECT s.name AS schema_name, o.name AS object_name, o.type AS object_type FROM sys.objects o JOIN sys.schemas s ON o.schema_id=s.schema_id WHERE o.is_ms_shipped=0"
    )
    return {
        "shape": sorted(shape, key=lambda r: json.dumps(r, sort_keys=True)),
        "rows": rows,
        "objects": sorted(inventory, key=lambda r: json.dumps(r, sort_keys=True)),
    }


@weaver_test(remote=True, resources={"rest", "tds"})
def test_live_warehouse_preview_and_bundle_share_plan_without_writes(
    fabric_workspace_item,
    fabric_credential,
    tmp_path,
    monkeypatch,
):
    workspace = Workspace(
        workspace=fabric_workspace_item.name, catalogue="Warehouse/PYTEST_WEAVER"
    )
    physical = os.environ.get("WEAVER_PYTEST_WAREHOUSE", "PYTEST_WH_1")
    with ConsoleSession(
        workspace=workspace, credential=fabric_credential, progress=False
    ) as session:
        register_session(session)
        guards = guard_preview(session, monkeypatch)
        occupancy = read_target_occupancy(catalogue_connection(session, workspace))
        owners = occupancy.get(("warehouse", physical.casefold()), ())
        assert len(owners) <= 1, owners
        logical = str(next(iter(owners))) if owners else "Warehouse/Preview"
        assert logical.startswith("Warehouse/")
        root = tmp_path / "source"
        _write(root, f"{logical}/schemas/Preview.yml", _schema("Preview"))
        _write(
            root,
            f"{logical}/Preview.Customer.sql",
            _warehouse_table("Preview.Customer"),
        )
        selector = f"{logical}=Warehouse/{physical}"
        target = WarehouseTarget(warehouse=ItemRef(physical))
        control = WarehouseTarget(warehouse=ItemRef("PYTEST_WEAVER"))
        before = {
            "target": warehouse_snapshot(session, workspace, target),
            "catalogue": warehouse_snapshot(session, workspace, control),
        }
        source_hashes = {
            str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in root.rglob("*")
            if p.is_file()
        }
        planned = weaver.build(root, items=selector, session=session, dry_run=True)
        assert (
            planned.succeeded
            and not planned.installation
            and planned.bundle_path is None
        )
        exported = weaver.build(
            root,
            items=selector,
            session=session,
            bundle_only=True,
            bundle_path=tmp_path / "bundle",
        )
        bundle = load_bundle(Location(exported.bundle_path), store=FilesystemStore())
        assert planned.preview.plan == bundle.plan
        assert planned.bundle_id == exported.bundle_id
        assert any(
            a["classification"] == "create"
            and a["physical_change"]["name"] == "Preview.Customer"
            for a in planned.preview.to_mapping()["actions"]
        )
        after = {
            "target": warehouse_snapshot(session, workspace, target),
            "catalogue": warehouse_snapshot(session, workspace, control),
        }
        assert before == after
        assert not guards["forbidden_calls"]
        assert source_hashes == {
            str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in root.rglob("*")
            if p.is_file()
        }
        result = {
            "preview": planned.to_mapping(),
            "bundle_id": exported.bundle_id,
            "plan_parity": True,
            "state_unchanged": before == after,
            "before": before,
            "after": after,
            "source_hashes": source_hashes,
            "guards": guards,
            "workspace": workspace.workspace,
            "physical_target": physical,
            "logical_item": logical,
            "environment": None,
        }
        destination = os.environ.get("WEAVER_PREVIEW_LIVE_RESULT")
        if destination:
            Path(destination).write_text(
                json.dumps(result, indent=2, default=str) + "\n"
            )

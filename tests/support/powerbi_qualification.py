"""Fixed-item Power BI qualification through public Build and Session APIs."""

import hashlib
import json
import shutil
from pathlib import Path

import weaver
from weaver.catalogue.connection import catalogue_connection
from weaver.catalogue.reader import read_table
from weaver.catalogue.render import InstallationScope, render_delete_scope
from weaver.catalogue.tables import (
    CURRENT_STATE_TABLES,
    LOAD_STATISTIC,
    LOG,
    PROJECTED_TABLES,
)
from weaver.catalogue.tsql import literal
from weaver.report_definition import verify_report
from weaver.semantic_models.definition import decode_model

OWNED_TABLES = (*PROJECTED_TABLES, *CURRENT_STATE_TABLES, LOAD_STATISTIC)


def save(output, name, value):
    (output / f"{name}.json").write_text(
        json.dumps(value, default=str, indent=2) + "\n", encoding="utf-8"
    )


def tree_hashes(root):
    return {
        p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


def scoped_rows(connection, scopes):
    return {
        str(scope): {
            table.name: read_table(connection, table, scope=scope)
            for table in OWNED_TABLES
        }
        for scope in scopes
    }


def cleanup_catalogue(connection, scopes, log_predicate, original_log_ids):
    statements = [
        render_delete_scope(table, scope=scope)
        for scope in scopes
        for table in OWNED_TABLES
    ]
    added = {
        row["log_sk"] for row in read_table(connection, LOG, predicate=log_predicate)
    } - original_log_ids
    statements.extend(
        f"DELETE FROM [_].[Log] WHERE [Log SK] = {literal(key)};"
        for key in sorted(added)
    )
    connection.execute("\n".join(statements))


def verify_published_rows(rows, repository, model, binding):
    from dataclasses import replace

    from weaver.catalogue.powerbi import project_report
    from weaver.declaration.model import WeaverItemId

    table_order = repository.semantic_models[WeaverItemId.parse(model)].table_names
    observed = rows[model]["SemanticModelTable"]
    assert (
        tuple(
            r["table_name"] for r in sorted(observed, key=lambda r: r["table_ordinal"])
        )
        == table_order
    )
    assert sorted(r["table_ordinal"] for r in observed) == list(
        range(1, len(table_order) + 1)
    )
    for item, report in repository.reports.items():
        if str(item) not in rows:
            continue
        report = replace(report, binding=binding)
        expected = project_report(item, report)
        actual = rows[str(item)]
        for table, records in expected.items():
            assert len(actual[table]) == len(records), (
                f"{item}: {table} row count differs"
            )
            for record in records:
                assert any(
                    all(row.get(k) == v for k, v in record.items())
                    for row in actual[table]
                ), f"{item}: {table} row differs"
        (installed,) = actual["Installation"]
        assert installed["signature"] == report.signature
        assert installed["workspace_id"] and installed["item_id"]


def exercise_catalogue(trial, model, reports, build_phase):
    from weaver.declaration.model import WeaverDocumentId, WeaverItemId
    from weaver.declaration.repository import parse_item_repository
    from weaver.locations import Location

    repository = parse_item_repository(Location(trial.as_posix()))
    model_root = WeaverDocumentId.model_root(WeaverItemId.parse(model))
    report_roots = {
        WeaverDocumentId.report_root(WeaverItemId.parse(item)) for item in reports
    }
    unchanged = build_phase("unchanged")
    assert set(unchanged.selection.selected_for_build) == {model_root, *report_roots}
    assert not unchanged.selection.impact.changed
    report_selectors = [f"{item}=Report/{target}" for item, target in reports.items()]
    assert not build_phase(
        "report-unchanged", items=report_selectors
    ).selection.selected_for_build
    item = next(iter(reports))
    report = repository.reports[WeaverItemId.parse(item)]
    path = trial / report.path / "definition/report.json"
    authored = json.loads(path.read_text(encoding="utf-8-sig"))
    settings = authored.setdefault("settings", {})
    settings["useStylableVisualContainerHeader"] = not settings.get(
        "useStylableVisualContainerHeader", False
    )
    path.write_text(
        json.dumps(authored, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    assert set(
        build_phase("report-edit", items=report_selectors).selection.selected_for_build
    ) == {WeaverDocumentId.report_root(WeaverItemId.parse(item))}
    assert not build_phase(
        "report-fixed", items=report_selectors
    ).selection.selected_for_build
    (overlay,) = trial.rglob(f"{model.split('/', 1)[1]}.tmdl")
    text = overlay.read_text(encoding="utf-8")
    from weaver.semantic_models import TmdlDefinition

    if "model Model" not in text:
        text += "\nmodel Model\n"
    definition = TmdlDefinition({"definition/model.tmdl": text.encode("utf-8")})
    definition.model.description = "Stage 3 qualification model edit"
    overlay.write_bytes(definition.parts["definition/model.tmdl"])
    selected = set(build_phase("model-edit").selection.selected_for_build)
    assert selected == {model_root, *report_roots}
    repeated = build_phase("model-fixed")
    assert set(repeated.selection.selected_for_build) == selected
    assert not repeated.selection.impact.changed


def qualify(*, source, output, session, model, model_target, reports):
    source, output = Path(source), Path(output)
    output.mkdir(parents=True, exist_ok=False)
    trial = output / "trial"
    shutil.copytree(source, trial)
    before = tree_hashes(source)
    scopes = tuple(InstallationScope(*item.split("/", 1)) for item in (model, *reports))
    connection = catalogue_connection(session) if session.workspace.catalogue else None
    log_predicate = " OR ".join(
        f"([Target type] = {literal(kind)} AND [Target name] = {literal(target)})"
        for kind, target in (
            ("SemanticModel", model_target),
            *(("Report", t) for t in reports.values()),
        )
    )
    original_log_ids = set()
    if connection is not None:
        rows = scoped_rows(connection, scopes)
        save(output, "catalogue-before", rows)
        assert not any(
            entries for tables in rows.values() for entries in tables.values()
        ), "Qualification scopes already hold catalogue rows"
        original_log_ids = {
            row["log_sk"]
            for row in read_table(connection, LOG, predicate=log_predicate)
        }
    semantic = session.semantic_model(model_target)
    history = semantic.power_bi.get_json(f"{semantic.dataset_path}/refreshes?$top=1")
    save(output, "refresh-before", history)
    assert all(
        entry["status"] in {"Completed", "Failed", "Cancelled", "Disabled"}
        for entry in history["value"]
    ), "Model refresh is active; wait before qualification"
    report_clients = {
        item: session.report_item(target) for item, target in reports.items()
    }
    originals = {
        model: semantic.get_definition(),
        **{item: client.get_definition() for item, client in report_clients.items()},
    }
    save(output, "definitions-before", originals)
    selectors = [
        f"{model}=SemanticModel/{model_target}",
        *(f"{item}=Report/{target}" for item, target in reports.items()),
    ]
    original_execute = session.execute_mutation
    phase = "baseline"

    def capture(plan, payloads=None, **options):
        save(output, f"{phase}-plan", plan.to_mapping())
        return original_execute(plan, payloads, **options)

    session.execute_mutation = capture

    def build_phase(name, *, items=None, active_session=session):
        nonlocal phase
        phase = name
        result = weaver.build(
            trial, items=selectors if items is None else items, session=active_session
        )
        save(output, name, result.to_mapping())
        save(output, f"{name}-actions", result.installation_report.to_mapping())
        assert result.succeeded, f"{name} Build failed"
        if connection is not None:
            from weaver.declaration.repository import parse_item_repository
            from weaver.locations import Location

            rows = scoped_rows(connection, scopes)
            save(output, f"{name}-catalogue", rows)
            physical_model = session.resolve_item(
                model_target, item_type="SemanticModel"
            )
            verify_published_rows(
                rows,
                parse_item_repository(Location(trial.as_posix())),
                model,
                {
                    "workspace_id": physical_model.workspace_id,
                    "item_id": physical_model.id,
                },
            )
        return result

    try:
        build_phase("baseline")
        if connection is None:
            build_phase("catalogue-free-repeat")
        else:
            exercise_catalogue(trial, model, reports, build_phase)
            save(output, "catalogue-qualified", scoped_rows(connection, scopes))
        save(
            output,
            "qualification",
            {"qualified": True, "catalogue": connection is not None},
        )
    finally:
        session.execute_mutation = original_execute
        errors = []
        try:
            semantic.update_definition(
                originals[model], allow_purge_data=True, timeout=900
            )
            assert decode_model(semantic.get_definition()) == decode_model(
                originals[model]
            )
        except Exception as exc:
            errors.append(f"{model}: {exc}")
        for item, client in report_clients.items():
            try:
                client.update_definition(originals[item], timeout=900)
                verify_report(originals[item], client.get_definition())
            except Exception as exc:
                errors.append(f"{item}: {exc}")
        if connection is not None:
            try:
                session.flush()
                cleanup_catalogue(connection, scopes, log_predicate, original_log_ids)
                rows = scoped_rows(connection, scopes)
                save(output, "catalogue-after", rows)
                assert not any(
                    entries for tables in rows.values() for entries in tables.values()
                )
                assert {
                    row["log_sk"]
                    for row in read_table(connection, LOG, predicate=log_predicate)
                } == original_log_ids
            except Exception as exc:
                errors.append(f"catalogue: {exc}")
        if tree_hashes(source) != before:
            errors.append("Source bytes changed")
        save(
            output,
            "cleanup",
            {"restored": not errors, "errors": errors, "source_hashes": before},
        )
        assert not errors, errors

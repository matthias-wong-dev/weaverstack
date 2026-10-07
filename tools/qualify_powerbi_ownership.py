"""Qualify fixed Power BI items in the approved pytest workspaces."""

import argparse
import json
import sys
from dataclasses import replace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from support.powerbi_qualification import qualify

import weaver
from weaver.declaration.model import WeaverItemId
from weaver.declaration.repository import parse_item_repository
from weaver.fabric.resolution import FabricResolver
from weaver.locations import Location
from weaver.report_definition import encode_report
from weaver.sessions import ConsoleSession
from weaver.workspaces import Workspace


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--workspace",
        choices=("PYTEST_WORKSPACE", "PYTEST_WORKSPACE_EXT"),
        required=True,
    )
    parser.add_argument("--model", default="SemanticModel/OwnershipQualification")
    parser.add_argument("--model-target", required=True)
    parser.add_argument(
        "--report", action="append", required=True, help="Report/logical=physical"
    )
    parser.add_argument("--catalogue", help="Existing catalogue Warehouse name")
    parser.add_argument("--environment", default="Weaver")
    parser.add_argument(
        "--provision",
        action="store_true",
        help="Create missing Reports through initialise",
    )
    args = parser.parse_args()
    reports = dict(value.split("=", 1) for value in args.report)
    if len(reports) != len(args.report):
        parser.error("Report bindings must be unique")
    for item in reports:
        if WeaverItemId.parse(item).item_type != "Report":
            parser.error("--report requires a Report logical identity")
    workspace = Workspace(
        workspace=args.workspace,
        catalogue=f"Warehouse/{args.catalogue}" if args.catalogue else None,
    )
    resolver = FabricResolver(workspace)
    with ConsoleSession(
        workspace=workspace, resolver=resolver, progress=False
    ) as session:
        if args.provision:
            if not args.catalogue:
                parser.error("--provision requires --catalogue")
            model = session.resolve_item(args.model_target, item_type="SemanticModel")
            repository = parse_item_repository(Location(args.source.as_posix()))
            definitions = {
                target: encode_report(
                    replace(
                        repository.reports[WeaverItemId.parse(item)],
                        binding={
                            "workspace_id": model.workspace_id,
                            "item_id": model.id,
                        },
                    )
                )
                for item, target in reports.items()
            }
            result = weaver.initialise(
                args.output.parent / f"{args.output.name}-provision",
                workspace=args.workspace,
                catalogue=args.catalogue,
                environment=args.environment,
                semantic_model=args.model_target,
                reports=definitions,
                install_weaver=False,
                client=resolver.client,
                session=session,
            )
            record = args.output.parent / f"{args.output.name}-provision.json"
            record.write_text(
                json.dumps(result.to_mapping(), default=str, indent=2), encoding="utf-8"
            )
            assert all(r.status in {"created", "existing"} for r in result.resources), (
                result.to_mapping()
            )
        qualify(
            source=args.source,
            output=args.output,
            session=session,
            model=args.model,
            model_target=args.model_target,
            reports=reports,
        )


if __name__ == "__main__":
    main()

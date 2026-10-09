"""Qualify fixed Power BI items in the approved pytest workspaces."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from support.powerbi_qualification import qualify

from weaver.declaration.model import WeaverItemId
from weaver.fabric.resolution import FabricResolver
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

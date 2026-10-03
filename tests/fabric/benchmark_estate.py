"""Time a representative estate's Build, no-op Build, Mirror and Wipe in Fabric.

Usage:
    python tests/fabric/benchmark_estate.py warehouse 1000 [build,noop,mirror,wipe]
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def main(argv) -> int:
    from support.fabric_performance import qualified_execution, run_estate

    from weaver.fabric.auth import desktop_credential, use_credential
    from weaver.sessions import ConsoleSession
    from weaver.workspaces import Workspace

    engine, declarations = argv[0], int(argv[1])
    operations = tuple(argv[2].split(",")) if len(argv) > 2 else None
    workspace = os.environ.get("WEAVER_FABRIC_WORKSPACE", "PYTEST_WORKSPACE")
    use_credential(desktop_credential())
    options = {} if operations is None else {"operations": operations}
    environment = os.environ.get("WEAVER_FABRIC_ENVIRONMENT", "weaver")
    with ConsoleSession(
        workspace=Workspace(
            workspace=workspace,
            environment=environment,
            execution=qualified_execution(),
        ),
        progress=False,
    ) as session:
        run = run_estate(
            engine,
            declarations,
            session=session,
            workspace_name=workspace,
            environment=environment,
            **options,
        )
    print(run.describe())
    return 0 if all(t.succeeded for t in run.timings) else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

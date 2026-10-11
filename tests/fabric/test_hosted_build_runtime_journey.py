"""Published hosted Build executes a sealed carrier from desktop and notebook."""

import io
import json
import os
import zipfile
from pathlib import Path
from unittest.mock import patch

from support.build_envs import LAKEHOUSE_JOURNEY_FIXTURE
from support.weaver_test import weaver_test

import weaver
from weaver.errors import BuildError
from weaver.fabric.onelake import abfss_root
from weaver.sessions.hosted_runtime import identity
from weaver.sessions.program import FabricProgram
from weaver.targets import ItemRef


@weaver_test(remote=True, resources={"rest", "tds", "livy", "onelake"})
def test_published_runtime_executes_whole_build_without_runtime_transport(
    fabric_workspace,
    fabric_catalogue,
    fabric_target_lakehouse,
    fabric_staging_lakehouse,
    fabric_initialise_catalogue,
    weaver_session,
    tmp_path,
    monkeypatch,
):
    assert os.environ.get("WEAVER_PYTEST_INJECT_WEAVER") == "0"
    session = weaver_session
    resolver, store = (
        session.resolver(fabric_workspace),
        session.store(fabric_workspace),
    )
    target = fabric_target_lakehouse.name
    selector = f"Lakehouse/Sales=Lakehouse/{target}"
    source = LAKEHOUSE_JOURNEY_FIXTURE.disposable(tmp_path / "source")
    stage = resolver.files_root(ItemRef(fabric_staging_lakehouse.name)).join(
        "hosted-build-source"
    )
    records = {"expected_runtime": identity()}
    from weaver.fabric.onelake import OneLakeDfsClient

    original_write = OneLakeDfsClient.write
    carriers = []

    def capture(client, location, data):
        if location.name == "carrier.zip":
            with zipfile.ZipFile(io.BytesIO(data)) as archive:
                names = archive.namelist()
                assert not any(
                    name.startswith("runtime/") or name.endswith(".whl")
                    for name in names
                )
                carriers.append(
                    {
                        "bytes": len(data),
                        "members": names,
                        "request": json.loads(archive.read("request.json")),
                    }
                )
        return original_write(client, location, data)

    monkeypatch.setattr(OneLakeDfsClient, "write", capture)
    records["transport"] = carriers
    attempted = False
    try:
        fabric_initialise_catalogue()
        observed = session.require_build_runtime(workspace=fabric_workspace)
        assert observed == records["expected_runtime"]
        records["observed_runtime"] = observed
        wrong = dict(
            records["expected_runtime"],
            version=records["expected_runtime"]["version"] + "1",
        )
        with patch("weaver.sessions.hosted_runtime.identity", return_value=wrong):
            try:
                session.require_build_runtime(workspace=fabric_workspace)
            except BuildError as error:
                records["mismatch_refused"] = str(error)
            else:
                raise AssertionError(
                    "Published runtime accepted a different development fingerprint"
                )
        attempted = True
        built = weaver.build(str(source.path), items=[selector], session=session)
        assert built.succeeded, built.errors
        records["desktop_report"] = built.installation_report.to_mapping()
        mutations = list(session.archive_mutations)
        assert mutations and all(row["status"] == "completed" for row in mutations)
        assert carriers and all(c["request"]["runtime"] == observed for c in carriers)
        records["desktop_carriers"] = mutations
        repeat = weaver.build(str(source.path), items=[selector], session=session)
        assert repeat.succeeded and not tuple(
            repeat.installation_report.action_results()
        )
        records["repeat_no_op"] = True
        weaver.wipe([f"Lakehouse/{target}"], session=session, unbind=True)
        for path in source.path.rglob("*"):
            if path.is_file():
                store.write(
                    stage.join(*path.relative_to(source.path).parts), path.read_bytes()
                )
        remote = (
            abfss_root(
                fabric_staging_lakehouse.workspace_id, fabric_staging_lakehouse.id
            )
            + "/Files/hosted-build-source"
        )
        options = json.dumps(
            {
                "source": remote,
                "workspace": fabric_workspace.workspace,
                "catalogue": fabric_catalogue.name,
                "environment": str(fabric_workspace.environment),
                "selector": selector,
            }
        )
        body = f"""import json
import weaver
from weaver.sessions import NotebookSession
from weaver.workspaces import Workspace
from weaver.sessions.hosted_runtime import identity
_options = json.loads({options!r})
with NotebookSession(spark=spark, workspace=Workspace(workspace=_options["workspace"], catalogue="Warehouse/" + _options["catalogue"], environment=_options["environment"])) as native:
    result = weaver.build(_options["source"], items=[_options["selector"]], session=native)
    emit({{"succeeded": result.succeeded, "runtime": identity(), "report": result.installation_report.to_mapping()}})
"""

        def call():
            from pyspark.sql import SparkSession

            emitted = []
            exec(
                body, {"spark": SparkSession.getActiveSession(), "emit": emitted.append}
            )
            assert len(emitted) == 1
            return emitted[0]

        program = FabricProgram("hosted_native_build", call, body, resubmit=False)
        native = session.execute_python(program, workspace=fabric_workspace)
        records["native"] = native
        assert native["succeeded"] and native["runtime"] == records["expected_runtime"]
    finally:
        if attempted:
            cleanup = weaver.wipe([f"Lakehouse/{target}"], session=session, unbind=True)
            records["cleanup"] = cleanup.to_mapping()
            assert any(
                i.target == f"Lakehouse/{target}" and i.outcome == "emptied"
                for i in cleanup.items
            )
        if store.exists(stage):
            store.delete(stage, recursive=True)
        output = os.environ.get("WEAVER_HOSTED_LIVE_RESULT")
        if output:
            Path(output).write_text(json.dumps(records, indent=2, default=str) + "\n")

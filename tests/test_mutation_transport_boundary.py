from dataclasses import replace
from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test
from test_mutation_archive_install import legacy

from weaver.locations import Location
from weaver.mutation import BoundTarget, PhysicalScope
from weaver.mutation.bundle import compute_bundle_id, write_bundle
from weaver.mutation.compatibility import compile_legacy_build
from weaver.mutation.executor import (
    Completed,
    MutationDriver,
    MutationExecutor,
    MutationJournal,
)
from weaver.sessions.install_archive import ArchiveStaging
from weaver.store import FilesystemStore


def bundle_at(tmp_path):
    plan, payloads = legacy()
    plan = compile_legacy_build(plan)
    plan = replace(
        plan,
        bundle_id="",
        execution=replace(plan.execution, spark_home_target_id="sales"),
    )
    plan = replace(plan, bundle_id=compute_bundle_id(plan))
    return write_bundle(
        Location(str(tmp_path / "bundle")),
        plan=plan,
        payloads=payloads,
        store=FilesystemStore(),
        allow_mutation=True,
    ), payloads


@weaver_test()
@pytest.mark.parametrize(
    "loss",
    ["none", "before-admission", "after-admission", "after-completion", "no-journal"],
)
@pytest.mark.parametrize("cleanup_failure", [False, True, "runtime"])
def test_generic_transport_recovers_action_evidence_and_never_falls_back(
    tmp_path, loss, cleanup_failure
):
    from weaver.sessions import install_archive
    from weaver.sessions.mutation_receipts import (
        DurableJournal,
        dumps,
        encode_report,
        loads,
    )

    assert hasattr(install_archive, "execute_mutation_in_scope"), (
        "generic archive transport is missing"
    )
    bundle, payloads = bundle_at(tmp_path)
    seen = []

    class Store(FilesystemStore):
        def delete(self, location, *, recursive=False):
            if cleanup_failure is True:
                raise OSError("cleanup unavailable")
            return super().delete(location, recursive=recursive)

    store = Store()

    class Resolver:
        def lakehouse(self, ref):
            seen.append(("resolve", ref))
            return Location(str(tmp_path / "staging"))

    def submit(source, **options):
        import zipfile

        seen.append(("submit", options))
        stage = next((tmp_path / "staging").rglob("carrier.zip")).parent
        output = str(stage / "result.json")
        with zipfile.ZipFile(stage / "carrier.zip") as archive:
            request = loads(archive.read("request.json"))
        context = {
            "archive_sha256": __import__("hashlib")
            .sha256((stage / "carrier.zip").read_bytes())
            .hexdigest(),
            "request": request,
        }
        sink = DurableJournal(
            lambda path, data: store.write(Location(path), data),
            output,
            bundle.bundle_id,
            request["invocation_id"],
            context=context,
        )
        if loss == "no-journal":
            FilesystemStore.delete(store, Location(output))
            raise TimeoutError("lost result")
        if loss == "before-admission":
            raise TimeoutError("lost result")
        if loss == "after-admission":
            from weaver.mutation.executor import LedgerEvent

            sink(
                (
                    LedgerEvent(
                        "dispatched",
                        "first",
                        0,
                        plan_id=bundle.bundle_id,
                        invocation_id=request["invocation_id"],
                    ),
                )
            )
            raise TimeoutError("lost result")
        report = MutationExecutor(
            {
                "folder": MutationDriver(lambda r: Completed()),
                "load_file": MutationDriver(lambda r: Completed()),
            },
            journal=MutationJournal(sink),
        ).execute(bundle.plan, payloads, invocation_id=request["invocation_id"])
        result = context | {
            "status": "completed",
            "plan_id": bundle.bundle_id,
            "invocation_id": request["invocation_id"],
            "report": encode_report(report),
        }
        if cleanup_failure == "runtime":
            result["runtime_cleanup_failure"] = {
                "path": "private/runtime",
                "error": "private cleanup failed",
            }
        data = dumps(result)
        store.write(Location(output), data)
        if loss == "after-completion":
            raise TimeoutError("lost final receipt")
        return {
            "bytes": len(data),
            "sha256": __import__("hashlib").sha256(data).hexdigest(),
        }

    scope = SimpleNamespace(transport_store=store, resolver=Resolver(), livy_run=submit)
    session = SimpleNamespace(
        scope=lambda workspace: scope,
        direct_delta_workers=2,
        archive_cleanup_failures=[],
        warnings=[],
    )
    if loss == "no-journal":
        from weaver.errors import InstallError

        with pytest.raises(InstallError, match="uncertain"):
            install_archive.execute_mutation_in_scope(
                session,
                bundle,
                staging=(
                    ArchiveStaging(
                        BoundTarget("stage", "lakehouse", "stage-id"),
                        "Files/authorised",
                    ),
                ),
                timeout=3,
            )
        report = None
    else:
        report = install_archive.execute_mutation_in_scope(
            session,
            bundle,
            staging=(
                ArchiveStaging(
                    BoundTarget("stage", "lakehouse", "stage-id"), "Files/authorised"
                ),
            ),
            timeout=3,
        )
    assert [e[0] for e in seen].count("submit") == 1
    options = next(e[1] for e in seen if e[0] == "submit")
    assert options["retry_submission"] is False
    assert options["timeout"] == 3 * sum(1 for _ in bundle.plan.actions())
    if loss == "no-journal":
        assert report is None
    elif loss == "before-admission":
        assert all(r.status == "not_dispatched" for r in report.results)
    elif loss == "after-admission":
        assert report.by_id["first"].status == "uncertain"
        assert report.by_id["second"].status == "not_dispatched"
        assert report.by_id["later"].status == "blocked"
    else:
        assert report.succeeded
    record = session.archive_mutations[-1]
    assert record["status"] == (
        "completed" if loss in {"none", "after-completion"} else "uncertain"
    )
    assert bool(session.archive_cleanup_failures) == (
        cleanup_failure and loss in {"none", "after-completion"}
    )
    assert bool(list((tmp_path / "staging").rglob("carrier.zip"))) == (
        bool(cleanup_failure) or loss not in {"none", "after-completion"}
    )


@weaver_test()
def test_no_safe_staging_declines_before_scope_or_submission(tmp_path):
    from weaver.sessions import install_archive

    assert hasattr(install_archive, "execute_mutation_in_scope"), (
        "generic archive transport is missing"
    )
    bundle, payloads = bundle_at(tmp_path)
    calls = []
    session = SimpleNamespace(
        scope=lambda workspace: calls.append(workspace),
        archive_cleanup_failures=[],
        direct_delta_workers=2,
    )
    assert (
        install_archive.execute_mutation_in_scope(
            session, bundle, staging=(PhysicalScope("sales", "Files/stage"),)
        )
        is None
    )
    assert calls == []
    assert session.archive_mutations[-1]["status"] == "declined"
    assert session.archive_mutations[-1]["mutated"] is False


@weaver_test()
@pytest.mark.parametrize(
    "custom", ["gate", "session", "store", "resolver", "livy", "executor", "credential"]
)
def test_session_owned_generic_route_declines_custom_capabilities_before_scope(
    tmp_path, custom
):
    from support.workspaces import given_workspace

    from weaver.sessions.console import ConsoleSession

    assert hasattr(ConsoleSession, "execute_mutation_archive"), (
        "gated Session route is missing"
    )

    class CustomSession(ConsoleSession):
        pass

    arguments = (
        {custom: object()}
        if custom in {"store", "resolver", "livy", "executor"}
        else {}
    )
    if custom == "credential":
        arguments["credential"] = SimpleNamespace(get_token=lambda *a, **k: None)
    session = (CustomSession if custom == "session" else ConsoleSession)(
        workspace=given_workspace(), **arguments
    )
    bundle, payloads = bundle_at(tmp_path)
    calls = []
    session.scope = lambda workspace: calls.append(workspace)
    result = session.execute_mutation_archive(
        bundle,
        staging=(PhysicalScope("stage", "Files/stage"),),
        allow_mutation=custom != "gate",
    )
    assert result is None
    assert calls == []
    assert session.archive_mutations[-1]["status"] == "declined"
    assert session.archive_mutations[-1]["mutated"] is False
    session.close()

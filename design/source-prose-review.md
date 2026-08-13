# Source prose review

This is a progress record for the Python source prose uplift. It is not a claim
that every module has completed editorial review, and it is not a prose-linter
specification.

## Status

Completed: `weaver_cli.main`, `weaver_cli.compose`, and `weaver_cli.shell`,
including their user-facing help, prompts, and rendering. The Fabric and Session
modules are reviewed for user-facing failures and their highest-volume module
essays. The build, catalogue, run, runtime, validation, declaration metadata,
reference resolution, folder runtime, and wipe boundaries also received targeted
module-prose review. The remaining declaration, SQL, Spark, storage, and
test-support modules still need a full comments and docstrings pass.

## User-facing boundaries

| Area | Status | Surfaces | Owning modules |
| --- | --- | --- |
| CLI commands | Complete | Help, option help, summaries, confirmations, retry prompts | `weaver_cli.main`, `weaver_cli.compose`, `weaver_cli.shell` |
| Configuration | In progress | Workspace configuration failures | `weaver.config` |
| Sessions | Reviewed boundary | Warnings, resource start state, timings | `weaver.session` |
| Build, load, and test | Partial | Reports, status labels, log links, targeted diagnostics | `weaver.build_bundle.report`, `weaver.load_report`, `weaver.test_report`, `weaver_cli.main` |
| Fabric | Reviewed boundary | Notebook, capacity, resource lookup, environment, Livy, and OneLake failures | `weaver.fabric` |
| Remaining core | Partial | Comments, docstrings, and module prose | `build_bundle`, `catalogue`, `declaration`, `run`, `runtime`, `spark`, `sql`, storage |

Runtime wording reports the detected state first. It gives a next step only
when the code has determined one. JSON output remains a data contract and is
not reformatted as part of editorial changes.

## Source prose conventions

Completed modules were reviewed for narrative comments, reviewer-facing
arguments, and implementation history. The same review remains pending for the
modules marked above. Comments should explain a platform constraint, invariant,
cache boundary, or failure mode that code cannot show. Docstrings should state a
callable or module contract. System-wide reasoning belongs in the relevant
document under `design/`.

For ongoing review, search changed Python code for rhetorical language and then
read the surrounding paragraph. The advisory Claude tripwire checks the most
common high-signal phrases; it does not replace editorial judgment.

## Related design ownership

| Source area | Design document |
| --- | --- |
| Session, build, install, and run handoffs | [Code architecture](code-architecture.md) |
| Build planning and installation constraints | [Build philosophy](build-philosophy.md) |
| Catalogue behaviour | [Central catalogue](catalogue.md) |
| Tests and Assumptions | [Validation](validation.md) |
| Fabric test boundaries | [Fabric integration tests](fabric-testing.md) |

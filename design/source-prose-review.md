# Source prose review

This records the completed Python source prose uplift. It is not a
prose-linter specification.

## Status

Completed: the `src/weaver/` and `src/weaver_cli/` trees were inventoried for
user-facing strings, long module prose, and high-signal rhetorical phrasing.
CLI help, prompts, errors, report types, session and Fabric boundaries, build,
catalogue, run, validation, declaration, SQL, Spark, storage, and test-support
modules received editorial review. Long explanations of system behaviour now
belong in the relevant document under `design/`.

## User-facing boundaries

| Area | Status | Surfaces | Owning modules |
| --- | --- | --- |
| CLI commands | Complete | Help, option help, summaries, confirmations, retry prompts | `weaver_cli.main`, `weaver_cli.compose`, `weaver_cli.shell` |
| Configuration | Complete | Workspace configuration failures | `weaver.config` |
| Sessions | Complete | Warnings, resource start state, timings | `weaver.session` |
| Build, load, and test | Complete | Reports, status labels, log links, targeted diagnostics | `weaver.build_bundle.report`, `weaver.load_report`, `weaver.test_report`, `weaver_cli.main` |
| Fabric | Complete | Notebook, capacity, resource lookup, environment, Livy, and OneLake failures | `weaver.fabric` |
| Core modules | Complete | Comments, docstrings, and module prose | `build_bundle`, `catalogue`, `declaration`, `run`, `runtime`, `spark`, `sql`, storage |

Runtime wording reports the detected state first. It gives a next step only
when the code has determined one. JSON output remains a data contract and is
not reformatted as part of editorial changes.

## Source prose conventions

Modules were reviewed for narrative comments, reviewer-facing arguments, and
implementation history. Comments explain a platform constraint, invariant, cache
boundary, or failure mode that code cannot show. Docstrings state a callable or
module contract. System-wide reasoning belongs in the relevant document under
`design/`.

For future review, search changed Python code for rhetorical language and read
the surrounding paragraph. The advisory Claude tripwire checks common
high-signal phrases; it does not replace editorial judgment.

## Related design ownership

| Source area | Design document |
| --- | --- |
| Session, build, install, and run handoffs | [Code architecture](code-architecture.md) |
| Build planning and installation constraints | [Build philosophy](build-philosophy.md) |
| Catalogue behaviour | [Central catalogue](catalogue.md) |
| Tests and Assumptions | [Validation](validation.md) |
| Fabric test boundaries | [Fabric integration tests](fabric-testing.md) |

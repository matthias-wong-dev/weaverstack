# Source prose review

This records the Python source prose uplift. It is not a prose-linter
specification.

## Status

Complete. The tree was inventoried for user-facing strings, long module prose,
and high-signal rhetorical phrasing. The final source-only pass reviewed the
inventoried comments and docstrings against their local constraints, removing
or shortening design rationale, implementation history, and reviewer-facing
explanations.

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

Comments must explain a platform constraint, invariant, cache boundary, or
failure mode that code cannot show. Docstrings state a callable or module
contract. System-wide reasoning belongs in the relevant document under `design/`.

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

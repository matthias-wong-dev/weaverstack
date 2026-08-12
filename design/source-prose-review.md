# Source prose review

This review covers the Python source tree as part of the writing uplift. It is
an editorial guide for ongoing maintenance, not a prose linter specification.

## User-facing boundaries

| Area | Surfaces reviewed | Owning modules |
| --- | --- | --- |
| CLI commands | Help, option help, summaries, confirmations, retry prompts | `weaver_cli.main`, `weaver_cli.compose`, `weaver_cli.shell` |
| Sessions | Warnings, resource start state, timings | `weaver.session` |
| Build, load, and test | Reports, status labels, log links, targeted diagnostics | `weaver.build_bundle.report`, `weaver.load_report`, `weaver.test_report`, `weaver_cli.main` |
| Fabric | Notebook, capacity, resource lookup, environment, Livy, and OneLake failures | `weaver.fabric` |
| Configuration | Workspace configuration failures | `weaver.config` |

Runtime wording reports the detected state first. It gives a next step only
when the code has determined one. JSON output remains a data contract and is
not reformatted as part of editorial changes.

## Source prose conventions

The source tree was reviewed for narrative comments, reviewer-facing arguments,
and implementation history. Comments remain where they explain a platform
constraint, invariant, cache boundary, or failure mode that code cannot show.
Docstrings state a callable or module contract. System-wide reasoning belongs in
the relevant document under `design/`.

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

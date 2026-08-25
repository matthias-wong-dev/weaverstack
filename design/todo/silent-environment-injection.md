# Silent environment injection

The agreed plan for making `weaver fabric environment publish` preserve
user-defined Environment content. **Nothing has landed yet.** This document is
expected to be deleted once the work does. For how Weaver works now, read
[code-architecture.md](../code-architecture.md).

## The defect

A Fabric Environment holds two library compartments: the `environmentYml`
(public packages, resolved from feeds at publish) and `customLibraries` (wheel,
jar, py and R files, installed verbatim). The current publish path treats the
yml as owned: whenever `deployment/fabric/environment.yml` differs from what is
published (`src/weaver/fabric/environment.py`, `publish_environment`), it
uploads its own file as a wholesale replacement. A user who added a package to
the same Environment through the Fabric UI loses it on Weaver's next publish.
Custom wheels are already safe: `delete_stale_wheels` only ever names
`weaverstack-*.whl`.

## Measured

A probe on 25 August 2026 against a throwaway Environment in the Weaver Example
workspace reproduced both halves end to end through the real REST API:

| Step | Result |
| --- | --- |
| User adds `networkx` via an ordinary yml, publishes | Success |
| Observation | published yml carries the comment-prefixed user definition verbatim |
| Weaver stages only its wheel, no yml call, publishes | Success |
| Observation | `environmentYml` byte-identical to before; wheel present in `customLibraries.wheelFiles` |

Conclusion: publish rebuilds the image from the whole manifest, so additive
staging alone preserves user definitions. No merge, no read-modify-write, no
ownership markers are needed.

## The design

Weaver never reads or writes `environmentYml`. Everything it needs crosses as
custom wheels:

```text
publish = stage(weaverstack wheel)
        + stage(dependency wheels: runtime deps minus desktop-only)
        + delete stale wheels among Weaver's known set
        + publish and poll
```

The dependency set comes from `[project].dependencies` in pyproject.toml minus
the desktop-only packages (`azure-identity`, `requests`, `build`,
`prompt_toolkit`). Today that is pyyaml, sqlparse and mssql-python, of which
only mssql-python is genuinely absent from Fabric Spark images.

This retires `deployment/fabric/environment.yml`. That file existed only to
compensate for staged wheels not pulling transitives; staging the transitives
explicitly closes the gap at the source. pyproject becomes the single
declaration of what Fabric needs.

## Workstreams

**W1. Dependency-wheel acquisition.** Resolve the Fabric dependency set into
concrete wheel files usable on Fabric's Linux Spark images:
`pip download --platform manylinux2014_x86_64 --python-version 3.11
--only-binary=:all:` (pure-Python wheels come out `py3-none-any`). Probe
mssql-python first: it ships native per-platform wheels and is the only
package where platform targeting is load-bearing. Cache per requirement-set
hash under a temporary directory; report resolved filenames and versions in
the result.

**W2. Publish flow rework.** In `publish_environment`: delete the wanted-yml
read/diff/upload branch and `upload_environment_yml`. Change detection moves
from yml text equality to wheel filename sets: changed when any expected
wheel is missing from `publishedLibraries.customLibraries.wheelFiles`.
Stale-wheel deletion widens by distribution name: for each package in the
Fabric dependency set plus `weaverstack`, remove any staged or published
wheel whose normalised name matches and whose filename differs from the one
just staged. Nothing outside that name set is ever deleted.

**W3. Definition retirement.** Delete `deployment/fabric/environment.yml`,
`ENVIRONMENT_DEFINITION`, `environment_dependencies`, `missing_from_environment`
and their exports; delete `tests/test_environment_definition_declaration.py`;
update the pyproject comment that describes the drift test; update AGENTS.md
and cli-usage.md where the two-definition story is described.

**W4. CLI surface.** `handle_environment_publish` needs no new options. The
result gains the staged wheel list and resolved versions; `dependencies_changed`
becomes `wheels_changed` (rename, since the meaning changes with it).

**W5. Tests.** Rework `tests/test_environment_install.py` fixtures to
wheel-manifest shapes; add: stale dep-wheel deletion removes only our
distribution names; idempotent rerun stages nothing; a foreign wheel and a
foreign yml survive a publish. Promote the 25 August probe to a marked
`fabric remote` acceptance test asserting preservation in one observation.

## Session injection for the Fabric suite

The publish-free path also changes how the Fabric suite itself runs. Today the
hosted half waits on `weaver fabric environment publish`, so iteration pays
minutes before any hosted test, and the published revision lags the checkout.

The arrangement:

1. `PYTEST_WORKSPACE` carries one Environment, `pytest_environment`, holding
   Weaver's dependencies only (pyyaml, sqlparse, mssql-python) and **no**
   weaver wheel. It is created and published once; it is republished only when
   the dependency set changes, which is rare.
2. The suite's shared Livy session carries the dev wheel across at start:
   build (~1 s), stage one file to Lakehouse Files over OneLake DFS (~1.5 s),
   create the session with both `spark.fabric.environmentDetails` (the
   dependency Environment) and `files: [abfss wheel]`, then one statement that
   extracts the wheel to `/tmp/weaver-dev` and prepends it to `sys.path`
   (~3 s). Livy statements share one interpreter, so the path prepend holds for
   every test in the session.
3. Nothing imports Weaver before that statement: the injection replaces
   `ensure_weaver`'s role for the suite.

Measured on the 35 South tenancy against `PYTEST_WORKSPACE`, 25 August 2026:

| variant | session start | inject cost | correctness |
| --- | --- | --- | --- |
| `files` + extract-to-`/tmp` | 125.6 s | 2.9 s | version flip verified; template readable; deps intact |
| `pyFiles` (zipimport) | 125.7 s | no statement | import works, but `SQL_TEMPLATE_DIR.is_dir()` is false under zipimport |
| `pip install --force-reinstall --no-deps` | 142.5 s | 7.6 s | works |

The extract variant is chosen. `pyFiles` is rejected on evidence: Weaver reads
its SQL templates through ``Path(__file__).parent``, which zipimport cannot
satisfy (`template_exists: false` above). One file crosses OneLake per run;
extraction lands on driver-local disk.

### Consequences

**The published-version-match test dies.** Whatever asserts that the session's
Weaver equals the Environment's published wheel goes; the session now
deliberately runs ahead of the Environment.

**The `hosted` marker loses its meaning.** It existed to say "needs a published
wheel". After this change nothing in the suite needs one: every Fabric test
runs against an injected dev wheel by default, and remote and hosted differ
only in where the body executes, not what must be installed first. The marker
is removed rather than redefined; `-m "fabric"` selects everything. Updates
follow in [AGENTS.md](../../AGENTS.md), [test-architecture.md](../test-architecture.md)
and [fabric-testing.md](../fabric-testing.md).

**Publish stops gating tests but keeps its product job.** `weaver fabric
environment publish` remains how a workspace's own notebooks get Weaver; the
suite simply no longer rides on it.

**W6. Harness work.** Deps-only Environment fixture (create if missing, publish
only when its yml differs); session-start injection replacing
`ensure_weaver`/publish preconditions in `tests/fabric`; delete the
version-match assertion; marker cleanup. Land W6 independently of W1-W5 —
it needs none of them.

### Open questions

**Notebook-driven tests.** A notebook attached to `pytest_environment` executes
with the Environment's libraries only, so a notebook body importing Weaver sees
no wheel at all. Any test that drives a notebook expecting Weaver code still
needs a publish, or the notebook body installs/stages its own copy. None exist
today; noted so the gap is a decision rather than a surprise.


## Sequence

```text
W1 probe (mssql-python fetch) --> W1 land --> W2 --> W3 + W5 fast tests --> W5 fabric acceptance

W6 (session injection for the suite) -- independent of W1-W5
```

W2 without W1 can land behind the existing three-package yml entries already
present in older Environments, but the clean cut lands W1 and W2 together.

## Risks

**Migration residue.** Environments published under the old model carry
pyyaml/sqlparse/mssql-python inside their yml. Weaver will not remove them;
they become harmless duplicates of the staged wheels until a person deletes
them from the definition. Document this in the release note for the change.

**Unpinned resolution.** Dep wheels resolve at publish time, so identical
source published later can carry newer dependency builds. Record resolved
versions in the result payload so drift is visible; decide on pinning later if
it bites.

**Name overlap.** If a user's yml pins the same package Weaver stages as a
wheel, both install and resolution order decides. Benign in practice,
ambiguous in theory; accepted.

## Open questions

**Pin policy for dep wheels.** Latest-at-publish keeps maintenance at zero;
a checked-in constraint file buys reproducibility at the cost of another file
to maintain. Defer until drift causes a real failure.

**--file mode.** Supplying a full environment definition to publish verbatim
alongside Weaver's wheels was discussed and deferred: nothing needs it yet,
and it composes cleanly later since it touches only the compartment Weaver
otherwise never writes.

## Considered and not planned

**Merge-on-publish with ownership marker blocks.** Solves automatic removals
of Weaver dependencies while preserving UI edits, at the cost of visible
ownership comments inside a file users consider theirs, text-level YAML
surgery, and subtler diffing. Rejected: the same removal property falls out of
name-scoped deletion in W2, because Weaver's footprint lives entirely in the
wheel manifest where ownership is unambiguous.

**Download-first merging.** Reading the published definition to feed it back
was safety machinery for replace semantics. With append-only staging there is
nothing to reconstruct, so the read goes rather than becoming optional.

**PyPI migration.** Once weaverstack is on PyPI, users declare
`weaverstack` in their own definition and feeds resolve transitively; this
machinery demotes to a developer/bootstrap tool. Nothing here blocks that
path; it is scaffolding for the pre-PyPI iteration loop.

## Appendix: probe evidence

The probe ran from this checkout against the live tenant, using Weaver's own
`FabricClient`, `find_workspace`, `publish_and_wait` and `upload_wheel`. It
simulated two publishes against one throwaway Environment: first a user adding
`networkx` through an ordinary yml (uploaded verbatim), then Weaver staging
only its wheel. The wheel was built from the working tree at the time; its
fingerprint version is unimportant.

Workspace: `Weaver Example`
Environment: `Weaver Probe Silent Injection` (`859d8c31-3428-460f-957a-32a8a3b283a3`),
left in place for inspection.

### The probe script

```python
"""Probe: does wheel-only staging preserve a user-defined environment?

Simulates two publishes against one throwaway Fabric Environment:
  A. a user adds networkx through an ordinary environment.yml
  B. Weaver stages ONLY its custom wheel, never touching the yml

If the published yml after B is byte-identical to after A while the wheel
appears alongside it, the silent-injection design holds.
"""

import json
import sys

import requests

from weaver.fabric.auth import prefer_cli_credential

prefer_cli_credential()

from weaver.fabric import environment as env_mod  # noqa: E402
from weaver.fabric.client import FabricClient  # noqa: E402
from weaver.fabric.resources import (  # noqa: E402
    ENVIRONMENT,
    ItemNotFoundError,
    find_item,
    find_workspace,
)

WORKSPACE = "Weaver Example"
ENV_NAME = "Weaver Probe Silent Injection"

USER_YML = """\
# An ordinary user-maintained environment definition.
dependencies:
  - pip:
      - networkx
"""


def upload_yml(env, text: str, client):
    url = f"{client.api_base_url}/workspaces/{env.workspace_id}/environments/{env.id}/staging/libraries"
    response = requests.post(
        url,
        headers={"Authorization": f"Bearer {client.token}"},
        files={"file": ("environment.yml", text.encode(), "application/octet-stream")},
        timeout=client.timeout,
    )
    if response.status_code not in (200, 201):
        raise SystemExit(f"yml upload failed: {response.status_code} {response.text[:300]}")


def observe(env, client, label: str) -> dict:
    body = env_mod.read_published(env, client=client)
    print(f"\n=== observation: {label} ===")
    print(json.dumps(body, indent=2))
    return body


def main() -> int:
    client = FabricClient()
    ws = find_workspace(WORKSPACE, client=client)
    print(f"workspace: {ws.name} ({ws.id})")

    # Clean slate: remove any leftover probe environment.
    try:
        stale = find_item(ws, ENV_NAME, item_type=ENVIRONMENT, client=client)
        print(f"removing leftover probe environment {stale.id}")
        client.request("DELETE", f"workspaces/{ws.id}/items/{stale.id}", expected=(200, 202, 204))
    except ItemNotFoundError:
        pass

    env, created = env_mod.find_or_create_environment(ws, ENV_NAME, client=client)
    print(f"environment: {env.name} ({env.id}) created={created}")

    # --- A. the user's state ----------------------------------------------
    upload_yml(env, USER_YML, client)
    state = env_mod.publish_and_wait(env, client=client, timeout=1200.0)
    print(f"publish A status: {state}")
    if state.lower() not in {"success", "succeeded"}:
        raise SystemExit(f"publish A finished {state!r}")
    obs_a = observe(env, client, "after user adds networkx")
    assert "networkx" in (obs_a.get("environmentYml") or ""), "user package missing after own publish"

    # --- B. future Weaver: wheel-only, yml untouched -----------------------
    root = env_mod.project_root()
    wheel = env_mod.build_wheel(root)
    print(f"\nstaging wheel only: {wheel.name}")
    env_mod.upload_wheel(env, wheel, client=client)
    state = env_mod.publish_and_wait(env, client=client, timeout=1800.0)
    print(f"publish B status: {state}")
    if state.lower() not in {"success", "succeeded"}:
        raise SystemExit(f"publish B finished {state!r}")
    obs_b = observe(env, client, "after weaver wheel-only publish")

    # --- verdict ------------------------------------------------------------
    yml_before = obs_a.get("environmentYml") or ""
    yml_after = obs_b.get("environmentYml") or ""
    wheels_after = env_mod.library_wheels(obs_b)

    preserved = yml_before == yml_after
    injected = wheel.name in wheels_after

    print("\n=== verdict ===")
    print(f"user yml byte-identical : {preserved}")
    print(f"weaver wheel installed   : {injected} ({wheels_after})")

    print("\nprobe environment left in place for inspection:")
    print(f"  {ENV_NAME} ({env.id}) in {ws.name}")

    return 0 if (preserved and injected) else 1


if __name__ == "__main__":
    sys.exit(main())
```

### The probe output

Verbatim, 25 August 2026:

```text
workspace: Weaver Example (88a7c7ad-7341-4a67-90b1-9935bb4440d9)
environment: Weaver Probe Silent Injection (859d8c31-3428-460f-957a-32a8a3b283a3) created=True
publish A status: Success

=== observation: after user adds networkx ===
{
  "customLibraries": {
    "wheelFiles": [],
    "pyFiles": [],
    "jarFiles": [],
    "rTarFiles": []
  },
  "environmentYml": "# An ordinary user-maintained environment definition.\ndependencies:\n  - pip:\n      - networkx\n"
}

staging wheel only: weaverstack-0.1.2.dev15567525581259023254-py3-none-any.whl
publish B status: Success

=== observation: after weaver wheel-only publish ===
{
  "customLibraries": {
    "wheelFiles": [
      "weaverstack-0.1.2.dev15567525581259023254-py3-none-any.whl"
    ],
    "pyFiles": [],
    "jarFiles": [],
    "rTarFiles": []
  },
  "environmentYml": "# An ordinary user-maintained environment definition.\ndependencies:\n  - pip:\n      - networkx\n"
}

=== verdict ===
user yml byte-identical : True
weaver wheel installed   : True (['weaverstack-0.1.2.dev15567525581259023254-py3-none-any.whl'])

probe environment left in place for inspection:
  Weaver Probe Silent Injection (859d8c31-3428-460f-957a-32a8a3b283a3) in Weaver Example
```

The `environmentYml` string in the second observation is byte-identical to the
first, including the comment line the user definition carried; the wheel list
grew by exactly the staged file. That is the whole claim of this plan,
observed once against the real service.

### Second probe: session injection costs (35 South, PYTEST_WORKSPACE)

Ran 25 August 2026. The dev wheel (587 KB) staged as one OneLake file and
carried into Livy sessions two ways, both alongside
`spark.fabric.environmentDetails` naming the dependency Environment:

```text
Variant A: files=[wheel], one extract statement
  session start            125.6 s   (the cost any hosted run pays today)
  extract + path prepend     2.9 s   one statement
  import resolves to         /tmp/weaver-dev/weaver/__init__.py
  metadata version           dev fingerprint, not the published one
  template_readable          true

Variant B: pyFiles=[wheel] (zipimport, no statement)
  session start            125.7 s
  import resolves to       ...container.../weaverstack-*.whl/weaver/__init__.py
  SQL_TEMPLATE_DIR.is_dir()  false  -- data files unreadable through a zip

Earlier variant, same tenancy: pip install --no-deps --force-reinstall from the
staged file: session start 142.5 s, install statement 7.6 s, version flip and
dependency health verified.

Baseline distribution check in the attached session: weaverstack (published),
sqlparse, pyyaml and mssql-python all resolve from Environment site-packages;
mssql-python is present only through the Environment.
```

The incremental cost of publish-free iteration is therefore roughly five
seconds per suite run: wheel build and upload before the session, one extract
statement after it. Session start dominates and is paid either way.






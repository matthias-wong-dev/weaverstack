# Releasing Weaver

One authored version, one command, and everything else derived from it or
checked against it.

## The whole manual procedure

```bash
# VERSION already contains the release we intend to make
python tools/release.py
```

There is no version argument. `VERSION` is the release, so there is nothing to
mistype and nothing to keep in step with it.

## The one authored version

`VERSION` at the repository root holds the release line under development:

```text
0.9.0
```

Nothing else in the repository carries a hand-maintained package version.
`pyproject.toml` keeps `dynamic = ["version"]` and derives it.

`hatch_build.py` reads `VERSION`, parses it with `packaging` and refuses a
spelling `packaging` would normalise, because `v0.9.0` or `0.09.0` would give a
tag and a wheel that disagree about the same release.

## What a checkout builds

```text
VERSION = 0.9.0

ordinary checkout                 0.9.0.dev<fingerprint>
clean HEAD tagged v0.9.0          0.9.0
clean HEAD tagged v0.9.1          error
clean HEAD tagged v0.8.0          error
dirty HEAD tagged v0.9.0          0.9.0.dev<fingerprint>
```

A release tag grants permission to drop the `.dev` suffix. It never decides
which release line the code is on, so **tagging changes no source file** and
the tagged source is byte for byte the source that was tested.

A dirty checkout is never the release, whatever it is tagged: the edits are
source the tag did not cover.

The fingerprint is unchanged from before. It is deterministic,
source-sensitive, stable for identical source, and free of a PEP 440 local
segment, because Fabric rejects a `+` in an uploaded wheel filename. Compare
these versions for equality, never to infer which source state is newer.

## What the tag triggers

Pushing `v*` is the release event. `.github/workflows/publish.yml`:

1. checks `GITHUB_REF_NAME == "v" + VERSION`, and fails before building if not;
2. runs ruff and the core suite;
3. builds the sdist and wheel;
4. reads the version back out of both artefacts and requires it to be exactly
   `VERSION`, in the metadata and in the filename
   (`tools/check_release_artefacts.py`);
5. runs `twine check`;
6. publishes to PyPI through trusted publishing, with no API token;
7. creates the GitHub Release for the same tag, with generated notes.

The GitHub Release is created only after PyPI publication succeeds, so a
Release never claims a publication that did not happen.

Fabric tests are not in the publishing path. They need a live workspace, a
running capacity and a credential, and the branch has already passed the
ordinary CI suite.

## What `tools/release.py` refuses

- a working tree with uncommitted changes;
- a branch other than `main`;
- a `HEAD` that does not match `origin/main`, because CI builds what the remote
  has;
- a tag that already exists on a different commit, because a version that
  reached PyPI cannot be replaced.

## Re-running a failed release

Pushing a tag that is already on the remote at the same commit updates no ref,
so GitHub raises no event and the workflow does not start again. `release.py`
detects that and stops, saying so:

```text
v0.9.0 is already pushed for this commit, so there is nothing to do.
Rerun the Publish to PyPI workflow in GitHub Actions to publish it.
```

Rerun the workflow from the Actions tab. If the source itself needs to change,
it is a different release: set `VERSION` to the next version and tag that,
because a version PyPI has accepted cannot be replaced.

## The chain into Fabric

```text
VERSION
   ↓
build metadata
   ↓
installed Weaver version   importlib.metadata.version("weaverstack")
   ↓
Fabric Environment requirement   weaverstack==0.9.0
```

A released publication pins the Environment to the publishing Weaver's own
version, so an Environment says which Weaver it holds. Readiness checks that
pin: an Environment carrying `weaverstack==0.9.1` is not ready for a `0.9.0`
client, and the ordinary Environment machinery republishes it.

Installed Weaver reads its version from package metadata and never reads
`VERSION` from a source checkout.

A development build has no PyPI counterpart to pin to, so it asks for the
distribution unpinned and accepts any named Weaver. That is what `weaver
initialise` from a checkout does, and it keeps giving the published Weaver.
`--dev` publication is unchanged: the checkout's content-addressed custom wheel
and no PyPI requirement.

## After a release

Preparing the next one is a single ordinary source change, in the first
post-release pull request:

```text
VERSION
0.9.1
```

Development builds become `0.9.1.dev<fingerprint>` from there.

## The invariant

At release time none of these can disagree silently:

```text
repository VERSION
Git tag
wheel metadata
sdist metadata
installed CLI version
Fabric Environment PyPI requirement
PyPI release
GitHub Release
```

`VERSION` is authored. Everything else is derived from it or checked against
it, and the release fails rather than publishing a disagreement.

# Fabric integration tests

These touch a real workspace and a running capacity. They are **deselected by
default** and skip unless a workspace is named, so nobody runs them by accident
and nobody without a tenant is blocked.

## Once

```bash
brew install azure-cli
az login
pip install -e '.[dev]'
```

`az login` is the only authentication Weaver needs — see
[CLI usage](cli-usage.md#signing-in-to-azure) for what it does and why the
credential chain is pinned.

You need a Fabric workspace you can create and delete items in. It can be empty;
the tests bring their own.

## Each session

Capacity is billed while it runs, so turn it on, work, turn it off.

```bash
weaver capacity resume  --resource-group <rg> --capacity-name <capacity>
weaver capacity status  --resource-group <rg> --capacity-name <capacity>

WEAVER_FABRIC_WORKSPACE=<workspace> .venv/bin/python -m pytest -m fabric

weaver capacity suspend --resource-group <rg> --capacity-name <capacity>
```

Resuming takes about half a minute and `resume` returns before the capacity is
`Active`, so `status` is the confirmation.

Without `WEAVER_FABRIC_WORKSPACE` the suite skips with a message saying so,
rather than failing.

## What the tests do to your workspace

They create their own Lakehouses, named `weavertest_<role>_<random>`, and delete
them in a `finally`. Nothing pre-existing is touched.

If a run is interrupted, the prefix makes leftovers obvious and they can be
deleted from the workspace by hand. Cleanup failures print a warning rather than
raising, so a tidy-up problem never masks a real test failure.

## The three test suites

| | command | needs |
|---|---|---|
| core | `pytest` | nothing — under a second |
| local Spark | `pytest -m spark` | a JDK and the `[spark]` extra |
| Fabric | `pytest -m fabric` | `az login`, a workspace, a running capacity |

The default run excludes both optional suites, so a contributor with neither a
JVM nor a tenant still gets a green build.

## Known Fabric behaviour

Things learned the hard way, kept here so they are not learned twice.

**OneLake does not support directory rename.** `PUT ?resource=directory` with
`x-ms-rename-source` returns `400 UnsupportedHeader`. Moving data on OneLake
means copying it — read the bytes and write them back — or `notebookutils.fs.mv`
from inside a session. `DELETE ?recursive=true` is supported.

This matters to Weaver's design: `Store.move_within_store` exists as a
first-class operation precisely so an implementation *can* choose a cheap
rename. On OneLake it cannot, and must copy.

**The Lakehouse SQL endpoint lags behind Delta schema changes.** After tables
appear or change, a Warehouse cross-database view can fail with
`Invalid object name` until the endpoint syncs. Force it with
`POST /v1/workspaces/{workspace}/sqlEndpoints/{endpoint}/refreshMetadata`,
taking the endpoint id from the Lakehouse's
`properties.sqlEndpointProperties.id`.

**Delta row counts without Spark.** Sum `numRecords` from `add.stats` across the
active files in `Tables/<schema>/<table>/_delta_log/*.json`, subtracting
`remove`d paths. Useful for asserting against Fabric without paying for a
session.

**A capacity resume is not instant.** About 30 seconds, and the ARM call returns
before the state changes. Poll `status`.

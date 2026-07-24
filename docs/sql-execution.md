# Warehouse SQL execution

Weaver has one SQL execution contract and two explicit connection modes. The
split is at the caller boundary, not inside statement execution:

```text
desktop Azure credential ─┐
                          ├── SqlConnectionPool ── PooledSqlExecutor
Fabric session identity ──┘
```

`PooledSqlExecutor` owns statement execution, parameter passing, result
handling, cursor closure, commit, and rollback. Neither authentication mode
duplicates those behaviours.

## Desktop cross-boundary mode

`weaver.fabric.desktop_sql_executor()` is for the CLI, development scripts, and
Fabric integration-test setup running on a desktop. The caller selects or
injects the Azure credential. Each new physical connection asks that credential
for:

```text
https://database.windows.net/.default
```

Core never calls `prefer_cli_credential()`. The desktop CLI and Fabric pytest
infrastructure own that policy.

## Fabric within-host mode

`weaver.fabric.fabric_sql_executor()` is for installed Weaver running inside a
Fabric notebook or Livy session. It uses:

```python
notebookutils.credentials.getToken("https://database.windows.net/")
```

It never uses Azure CLI state or a desktop token cache. Calling this factory
outside a supported Fabric session fails explicitly.

Warehouse identity is still typed as
`workspace + Warehouse + item name`. The resolver obtains the item ID and its
TDS server through the Fabric Warehouse connection-string endpoint, then
produces the same `SqlEndpoint` record in either execution position. The
Warehouse display name is the database name.

## Driver contract and token refresh

Both modes call `mssql_python.connect()` with the validated connection string:

```python
(
    f"Server={server},1433;"
    f"Database={database};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
)
```

The current access token is UTF-16-LE encoded and passed through
`attrs_before[1256]` (`SQL_COPT_SS_ACCESS_TOKEN`). Authentication material is
requested when a physical connection is created, not when a pool is
constructed, so a replacement connection does not inherit an expired token.

Pools are bounded, thread-safe, endpoint-specific, and owned by one workflow or
session. A failed lease is discarded; closing the owner closes every pooled
connection.

## Legacy wipe source

The port was compared against the reviewed legacy Weaver baseline
`a97ba8a0b00dd66dff1b2c5e818403694562fd30` and the inspected sibling revision
`fee20251c14f4b5ef99ae8b30131123d4bd81cd6`.

The exact source locations are:

- `src/weaver_runtime/dbrep/sql/templates/admin/wipe.sql` — foreign-key and
  object enumeration plus dynamic drop statements;
- `src/weaver_runtime/dbrep/sql/backend.py::_drop_user_schemas` — non-system
  schema enumeration and cleanup;
- the older `source/sql_templates/admin/wipe.sql` — the original combined
  object-and-schema batch.

`generate_warehouse_wipe_sql()` is pure: it resolves nothing, authenticates
nothing, and opens no connection. The selected Warehouse name is never
interpolated into the generated SQL.

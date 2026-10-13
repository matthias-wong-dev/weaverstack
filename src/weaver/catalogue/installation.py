"""Resolve recorded Installation identities and replace its legacy schema."""

from uuid import UUID

from ..errors import BuildError
from ..targets import ItemRef
from .tables import INSTALLATION


def installation_schema_upgrade(connection):
    columns = connection.columns_of(INSTALLATION)
    if columns is None:
        return None
    nullability = connection.nullability_of(INSTALLATION)
    if any(
        c.public_name.casefold() not in columns
        or nullability.get(columns.get(c.public_name.casefold())) == "YES"
        for c in INSTALLATION.columns
    ) or {"workspace id", "item id"} & set(columns):
        return {"columns": dict(nullability)}
    return None


def _guid(value, *, item, column):
    try:
        if (
            not isinstance(value, str)
            or str(UUID(value)).casefold() != value.casefold()
        ):
            raise ValueError
    except ValueError:
        raise BuildError(
            f"{item}: invalid {column} {value!r}; repair the installation record"
        ) from None
    return value.casefold()


def resolve_installations(catalogue, rows, *, resolver, items=()):
    upgrade = catalogue.installation_upgrade
    columns = {name.casefold() for name in upgrade["columns"]} if upgrade else set()
    physical_workspace = resolver.workspace
    selected = {str(item) for item in items}
    resolved_rows = []
    publication_rows = []
    seen = set()
    for original in rows:
        row = dict(original)
        item = f"{row['item_type']}/{row['item_name']}"
        key = (row["item_type"], row["item_name"])
        if key in seen:
            raise BuildError(
                f"{item}: duplicate installation records; repair the catalogue"
            )
        seen.add(key)
        workspace_id = row.get("workspace_id")
        if workspace_id is not None:
            if (
                _guid(workspace_id, item=item, column="Workspace GUID")
                != physical_workspace.id.casefold()
            ):
                raise BuildError(
                    f"{item}: Workspace GUID {workspace_id} does not match {physical_workspace.id}; use the installed workspace"
                )
        if upgrade or item in selected:
            target = resolver.resolve(
                ItemRef(row["target_name"]), item_type=row["item_type"]
            )
            item_id = row.get("item_id")
            # Old Warehouse/Lakehouse publication wrote the recorded target name
            # into Item ID. Only that exact old-column fallback is backfillable.
            name_fallback = (
                upgrade is not None
                and "item id" in columns
                and "target item guid" not in columns
                and item_id == row["target_name"]
            )
            if item_id is not None and not name_fallback:
                if (
                    _guid(item_id, item=item, column="Target item GUID")
                    != target.id.casefold()
                ):
                    raise BuildError(
                        f"{item}: Target item GUID {item_id} does not match {target.id}; repair the installation record"
                    )
            if upgrade:
                row.update(workspace_id=target.workspace_id, item_id=target.id)
                publication_rows.append(
                    {
                        **row,
                        "workspace_name": physical_workspace.name,
                        "target_name": target.name,
                    }
                )
        resolved_rows.append(row)
    if upgrade is not None:
        upgrade.update(rows=[dict(row) for row in rows], resolved=publication_rows)
    return tuple(resolved_rows)


def render_installation_upgrade(upgrade):
    from .tables import AUDIT_COLUMN_NAMES, public_column_name
    from .tsql import identifier, literal

    columns = upgrade["columns"]
    expected = {c.public_name for c in INSTALLATION.columns}
    audits = {public_column_name(name) for name in AUDIT_COLUMN_NAMES}
    unknown = set(columns) - expected - audits - {"Workspace ID", "Item ID"}
    mixed = ("Workspace ID" in columns and "Workspace GUID" in columns) or (
        "Item ID" in columns and "Target item GUID" in columns
    )
    if unknown or mixed:
        raise BuildError(
            "Installation has an unsupported column shape; repair the catalogue"
        )
    rows = upgrade.get("rows")
    resolved = upgrade.get("resolved")
    if rows is None or resolved is None:
        raise BuildError(
            "Installation upgrade needs every recorded target resolved; regenerate the bundle"
        )
    target = "[_].[Installation]"
    stage = "[_].[Installation_identity_stage]"
    key = "[Item type], [Item name]"

    def fail(message):
        return f"THROW 50000, {literal(message)}, 1;"

    def source_column(column):
        actual = column.public_name
        if actual not in columns:
            actual = {"workspace_id": "Workspace ID", "item_id": "Item ID"}.get(
                column.name, actual
            )
        return (
            f"s.{identifier(actual)}"
            if actual in columns
            else "CAST(NULL AS varchar(128))"
        )

    original_projection = ", ".join(source_column(c) for c in INSTALLATION.columns)
    aliases = ", ".join(identifier(c.public_name) for c in INSTALLATION.columns)
    observed = (
        "SELECT "
        + aliases
        + " FROM (VALUES "
        + ", ".join(
            "("
            + ", ".join(
                "CAST(" + literal(row.get(c.name)) + " AS varchar(128))"
                for c in INSTALLATION.columns
            )
            + ")"
            for row in rows
        )
        + f") AS observed ({aliases})"
        if rows
        else "SELECT "
        + ", ".join(
            "CAST(NULL AS varchar(128)) AS " + identifier(c.public_name)
            for c in INSTALLATION.columns
        )
        + " WHERE 1 = 0"
    )
    shape_conditions = " OR ".join(
        f"(name = {literal(name)} AND is_nullable = {1 if nullable == 'YES' else 0})"
        for name, nullable in sorted(columns.items())
    )
    steps = [
        f"IF OBJECT_ID({literal(target)}, N'U') IS NULL "
        + fail("Installation is missing; regenerate the bundle"),
        f"IF OBJECT_ID({literal(stage)}) IS NOT NULL "
        + fail("Installation stage already exists; repair the catalogue"),
        f"IF (SELECT COUNT_BIG(*) FROM sys.columns WHERE object_id = OBJECT_ID({literal(target)})) <> {len(columns)} "
        f"OR (SELECT COUNT_BIG(*) FROM sys.columns WHERE object_id = OBJECT_ID({literal(target)}) AND ({shape_conditions})) <> {len(columns)} "
        + fail("Installation schema changed since planning; regenerate the bundle"),
        f"IF EXISTS (SELECT 1 FROM {target} GROUP BY {key} HAVING COUNT_BIG(*) > 1) "
        + fail("Installation keys are not unique; repair the catalogue"),
        f"IF (SELECT COUNT_BIG(*) FROM {target}) <> {len(rows)} OR EXISTS (SELECT {original_projection} FROM {target} AS s EXCEPT {observed}) "
        + fail("Installation changed since planning; regenerate the bundle"),
    ]
    audit_names = [
        public_column_name(name)
        for name in AUDIT_COLUMN_NAMES
        if public_column_name(name) in columns
    ]
    definitions = [
        identifier(c.public_name) + " varchar(128) NOT NULL"
        for c in INSTALLATION.columns
    ]
    definitions += [
        identifier(name)
        + " datetime2(6) "
        + ("NULL" if columns[name] == "YES" else "NOT NULL")
        for name in audit_names
    ]
    steps.append(f"CREATE TABLE {stage} (" + ", ".join(definitions) + ");")
    all_columns = aliases + (
        ", " + ", ".join(identifier(name) for name in audit_names)
        if audit_names
        else ""
    )
    if resolved:
        mapping = (
            "(VALUES "
            + ", ".join(
                "("
                + ", ".join(
                    literal(row[name])
                    for name in (
                        "item_type",
                        "item_name",
                        "workspace_name",
                        "workspace_id",
                        "target_name",
                        "item_id",
                    )
                )
                + ")"
                for row in resolved
            )
            + ") AS r ([Item type], [Item name], [Workspace name], [Workspace GUID], [Target name], [Target item GUID])"
        )
        projection = ", ".join(
            (
                "r."
                if c.name
                in {"workspace_name", "workspace_id", "target_name", "item_id"}
                else "s."
            )
            + identifier(c.public_name)
            for c in INSTALLATION.columns
        )
        projection += (
            ", " + ", ".join("s." + identifier(name) for name in audit_names)
            if audit_names
            else ""
        )
        steps.append(
            f"INSERT INTO {stage} ({all_columns}) SELECT {projection} FROM {target} AS s JOIN {mapping} ON s.[Item type] = r.[Item type] AND s.[Item name] = r.[Item name];"
        )
    steps += [
        f"IF (SELECT COUNT_BIG(*) FROM {stage}) <> {len(rows)} "
        + fail("Installation copy did not preserve every row"),
        f"IF EXISTS (SELECT 1 FROM {stage} GROUP BY {key} HAVING COUNT_BIG(*) > 1) "
        + fail("Installation copy keys are not unique; regenerate the bundle"),
        f"DROP TABLE {target};",
        f"EXEC sys.sp_rename {literal(stage)}, N'Installation', N'OBJECT';",
        f"ALTER TABLE {target} ADD CONSTRAINT [PK_Installation] PRIMARY KEY NONCLUSTERED ({key}) NOT ENFORCED;",
    ]
    # Dynamic SQL binds legacy columns only when this exact snapshot is upgraded.
    body = "\n".join(step for step in steps if step)
    return (
        "BEGIN TRY\nBEGIN TRANSACTION;\nEXEC("
        + literal(body)
        + ");\nCOMMIT;\nEND TRY\nBEGIN CATCH\nIF @@TRANCOUNT > 0 ROLLBACK;\nTHROW;\nEND CATCH;"
    )

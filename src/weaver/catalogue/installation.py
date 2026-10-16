"""Resolve recorded Installation identities and replace its legacy schema."""

from uuid import UUID

from ..errors import BuildError, InstallationIdentityError
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
        raise InstallationIdentityError(
            f"{item}: invalid {column} {value!r}; repair the installation record"
        ) from None
    return value.casefold()


def verify_installation(row, *, resolver, backfillable=False):
    """The Fabric item an installation records, once its identity is confirmed.

    Names locate the workspace and item; the recorded GUIDs establish that they
    are still the ones the catalogue installed to. A renamed workspace with the
    same GUID is not a replacement, and only the configured name finds it. A
    legacy ``Item ID`` holding the target name is skipped when ``backfillable``.
    """

    from ..errors import CommandError
    from ..fabric.resources import ItemNotFoundError

    item = f"{row['item_type']}/{row['item_name']}"
    try:
        workspace = resolver.workspace
    except CommandError as exc:
        raise InstallationIdentityError(
            f"{item}: {exc} If it was renamed, update the workspace name in "
            "workspace configuration."
        ) from exc
    recorded = row.get("workspace_id")
    if (
        recorded is not None
        and _guid(recorded, item=item, column="Workspace GUID")
        != workspace.id.casefold()
    ):
        raise InstallationIdentityError(
            f"{item}: Workspace GUID {recorded} does not match {workspace.id}; use the installed workspace"
        )
    try:
        target = resolver.resolve(
            ItemRef(row["target_name"]), item_type=row["item_type"]
        )
    except ItemNotFoundError as exc:
        raise InstallationIdentityError(
            f"{item}: {exc} Restore it, or unbind {item}."
        ) from exc
    recorded = row.get("item_id")
    if (
        recorded is not None
        and not (backfillable and recorded == row["target_name"])
        and _guid(recorded, item=item, column="Target item GUID")
        != target.id.casefold()
    ):
        raise InstallationIdentityError(
            f"{item}: {row['item_type']} {target.name!r} has Target item GUID "
            f"{target.id}, but the catalogue installed to {recorded}. The item "
            f"was replaced; unbind {item} and build it again"
        )
    return target


def recorded_installation(catalogue, item):
    """The item's installation row, if it recorded Fabric GUIDs to compare.

    A row that predates the identity columns has none.
    """

    row = next(iter(catalogue.rows.get(item, {}).get(INSTALLATION.name, ())), None)
    return row if row and (row.get("workspace_id") or row.get("item_id")) else None


def check_installations(rows, *, resolver):
    """Verify each installation, returning ``(resolved, faults)`` by item key.

    A failing installation is a fault and does not stop the others. The
    workspace's items are listed once for the whole batch.
    """

    rows = tuple(rows)
    if len(rows) > 1 and hasattr(resolver, "discover_once"):
        resolver.discover_once()
    resolved = {}
    faults = {}
    for row in rows:
        key = (row["item_type"], row["item_name"])
        try:
            resolved[key] = verify_installation(row, resolver=resolver)
        except InstallationIdentityError as exc:
            faults[key] = exc
    return resolved, faults


def verify_installations(rows, *, resolver):
    """Resolved items by item key; the first inconsistent installation raises."""

    resolved, faults = check_installations(rows, resolver=resolver)
    if faults:
        raise next(iter(faults.values()))
    return resolved


def _unique(rows):
    seen = set()
    for row in rows:
        key = (row["item_type"], row["item_name"])
        if key in seen:
            raise InstallationIdentityError(
                f"{row['item_type']}/{row['item_name']}: duplicate installation records; repair the catalogue"
            )
        seen.add(key)
    return rows


def upgraded_installations(rows, *, resolver, columns):
    """Each legacy Installation row with the Fabric identity it now resolves to.

    ``columns`` is the legacy table's physical column names. Every row is
    verified, and returned with ``workspace_name``, ``workspace_id``,
    ``target_name`` and ``item_id`` set from Fabric.
    """

    folded = {name.casefold() for name in columns}
    # Old Warehouse/Lakehouse publication wrote the recorded target name into
    # Item ID. Only that exact old-column fallback is backfillable.
    backfillable = "item id" in folded and "target item guid" not in folded
    rows = _unique(tuple(dict(row) for row in rows))
    if len(rows) > 1 and hasattr(resolver, "discover_once"):
        resolver.discover_once()
    upgraded = []
    for row in rows:
        target = verify_installation(row, resolver=resolver, backfillable=backfillable)
        upgraded.append(
            {
                **row,
                "workspace_name": resolver.workspace.name,
                "workspace_id": target.workspace_id,
                "target_name": target.name,
                "item_id": target.id,
            }
        )
    return upgraded


def resolve_installations(catalogue, rows, *, resolver, items=()):
    """Verify the installations ``items`` need, or every one while upgrading.

    Only the legacy-schema upgrade replaces the whole table, so only it resolves
    every record. Otherwise an inconsistent installation nothing selected uses
    changes nothing.
    """

    upgrade = catalogue.installation_upgrade
    observed = [dict(row) for row in rows]
    rows = _unique(tuple(dict(row) for row in observed))
    if upgrade is not None:
        publication_rows = upgraded_installations(
            rows, resolver=resolver, columns=upgrade["columns"]
        )
        for row, published in zip(rows, publication_rows):
            row.update(
                workspace_id=published["workspace_id"], item_id=published["item_id"]
            )
        upgrade.update(rows=observed, resolved=publication_rows)
        return rows
    selected = {str(item) for item in items}
    wanted = [
        row for row in rows if f"{row['item_type']}/{row['item_name']}" in selected
    ]
    if len(wanted) > 1 and hasattr(resolver, "discover_once"):
        resolver.discover_once()
    for row in wanted:
        verify_installation(row, resolver=resolver)
    return rows


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

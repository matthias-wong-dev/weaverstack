"""Session-owned SQL endpoint and relation-shape acquisition."""

from ..catalogue.tsql import literal
from ..errors import BuildError
from ..fabric.resolution import _server_name
from ..sql import SqlEndpoint
from ..targets import ItemRef, WarehouseTarget


def semantic_source(
    session, item, *, item_type, schema, name, include_columns, workspace
):
    resolved = session.resolve_item(item, item_type=item_type, workspace=workspace)
    resolver = session.resolver(workspace)
    if item_type == "Warehouse":
        endpoint = resolver.sql_endpoint(WarehouseTarget(ItemRef(resolved.name)))
    elif item_type == "Lakehouse":
        body = resolver.client.get_json(
            f"workspaces/{resolved.workspace_id}/lakehouses/{resolved.id}"
        )
        properties = body.get("properties", {}).get("sqlEndpointProperties", {})
        if not properties.get("id") or not properties.get("connectionString"):
            raise BuildError(
                f"Lakehouse/{resolved.name}: SQL analytics endpoint is unavailable"
            )
        endpoint = SqlEndpoint(
            server=_server_name(properties["connectionString"]),
            database=properties["id"],
            workspace_id=resolved.workspace_id,
            warehouse_id=properties["id"],
            warehouse_name=resolved.name,
        )
    else:
        raise BuildError(
            f".source requires a Lakehouse or Warehouse, got {item_type!r}"
        )
    columns = []
    if include_columns:
        columns = list(
            session.query_tsql(
                "SELECT COLUMN_NAME AS column_name, DATA_TYPE AS data_type "
                "FROM INFORMATION_SCHEMA.COLUMNS "
                f"WHERE TABLE_SCHEMA = {literal(schema)} AND TABLE_NAME = {literal(name)} "
                "ORDER BY ORDINAL_POSITION",
                target=endpoint,
                workspace=workspace,
            )
        )
    return {
        "item_type": resolved.type,
        "item_name": resolved.name,
        "item_id": resolved.id,
        "workspace_id": resolved.workspace_id,
        "server": endpoint.server,
        # People create connections by database name, and binding matches the
        # name; item_id keeps the source's identity across renames.
        "database": resolved.name,
        "schema": schema,
        "object": name,
        "source_columns": columns,
    }

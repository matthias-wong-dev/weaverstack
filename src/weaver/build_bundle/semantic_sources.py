"""Resolve semantic source state before pure Build planning."""

from ..catalogue.claims import catalogue_columns
from ..declaration.metadata import TABLE, VIEW
from ..declaration.model import WeaverItemId
from ..errors import BuildError
from ..semantic_models.fragments import needs_source_columns, source_table
from ..semantic_models.references import source_identity
from ..targets import physical_item


def read_semantic_sources(
    repository, bindings, catalogue, *, session, workspace, inventories
):
    wanted = {}
    mappings = {}
    for item, contribution in sorted(
        repository.semantic_models.items(), key=lambda pair: str(pair[0])
    ):
        if item not in bindings.by_item or not contribution.source_references:
            continue
        tables = {
            name: source_table(contribution.parts, name)
            for name in contribution.source_references
        }
        for table, reference in contribution.source_references.items():
            logical = str(source_identity(reference).item)
            if mapping := contribution.expression_sources.get(logical):
                mappings[reference] = WeaverItemId.parse(mapping["target"])
            _require_managed(
                reference,
                contribution,
                table,
                repository=repository,
                bindings=bindings,
                catalogue=catalogue,
                workspace=workspace,
            )
            wanted[reference] = wanted.get(reference, False) or needs_source_columns(
                tables[table]
            )
    observed = {}
    rebuilt = None
    for reference, needs_columns in sorted(wanted.items()):
        identity = source_identity(reference)
        bound = bindings.by_item.get(identity.item)
        schema, name = catalogue_columns(identity)
        source_tables = catalogue.rows.get(identity.item, {})
        if not needs_columns:
            observed[reference] = {
                "reference": reference,
                "description": next(
                    (
                        r.get("description")
                        for r in source_tables.get("TableDictionary", ())
                        if r.get("schema_name") == schema
                        and r.get("object_name") == name
                    ),
                    None,
                ),
                "column_notes": {
                    r["column_name"]: r["description"]
                    for r in source_tables.get("ColumnDictionary", ())
                    if r.get("schema_name") == schema
                    and r.get("object_name") == name
                    and r.get("description")
                },
            }
            continue
        authored = repository.source_documents.get(identity) if bound else None
        kind = _managed_kind(identity, repository, bindings, catalogue)
        installed = catalogue.rows.get(identity.item, {}).get("Installation", ())
        if bound is not None:
            target_name = bound.target.item.name
        elif len(installed) == 1 and installed[0].get("target_name"):
            target_name = installed[0]["target_name"]
            if (
                reference not in mappings
                and identity.item in workspace.configured_items
            ):
                configured = physical_item(workspace.target_for(identity.item)).name
                if configured != target_name:
                    raise BuildError(
                        f"Semantic source {reference}: configured target {configured!r} is not the installed target {target_name!r}. Build {identity.item} first."
                    )
        else:
            raise BuildError(
                f"Semantic source {reference}: no installed target. Build {identity.item} first."
            )
        if reference in mappings:
            target = mappings[reference]
            if (
                target.item_type != identity.item.item_type
                or target.item_name != target_name
            ):
                raise BuildError(
                    f"Semantic source {reference}: mapped target {target} is not the managed target {identity.item.item_type}/{target_name}. Build the source into that target first."
                )
        schema, name = catalogue_columns(identity)
        source_tables = catalogue.rows.get(identity.item, {})
        description = next(
            (
                r.get("description")
                for r in source_tables.get("TableDictionary", ())
                if r.get("schema_name") == schema and r.get("object_name") == name
            ),
            None,
        )
        notes = {
            r["column_name"]: r["description"]
            for r in source_tables.get("ColumnDictionary", ())
            if r.get("schema_name") == schema
            and r.get("object_name") == name
            and r.get("description")
        }
        columns = []
        if authored is not None and authored.document.has_declared_schema:
            document = authored.document
            declared = (
                (document.identity_column,) if document.identity_column else ()
            ) + document.schema
            columns = [{"column_name": c.name, "data_type": c.type} for c in declared]
            if document.description.literal:
                description = document.description.literal
            notes.update(
                {c.name: c.note.literal for c in declared if c.note and c.note.literal}
            )
        # An inferred source this Build creates or rebuilds acquires its shape
        # only during installation. One it leaves in place has its installed shape.
        if needs_columns and not columns and bound is not None and authored is not None:
            if rebuilt is None:
                from .planner import select_items

                rebuilt = set(
                    select_items(
                        repository,
                        catalogue,
                        by_item=bindings.by_item,
                        inventories=inventories,
                    ).selected_for_build
                )
            if identity in rebuilt:
                raise BuildError(
                    f"Semantic source {reference}: source shape is unavailable before installation. Declare its schema or author semantic columns with dataType and sourceColumn."
                )
        metadata = session.semantic_source(
            target_name,
            item_type=identity.item.item_type,
            schema=identity.object_id.schema,
            name=identity.object_id.object,
            include_columns=needs_columns and not columns,
            workspace=workspace,
        )
        if needs_columns:
            columns = columns or metadata.get("source_columns", [])
            if not columns:
                raise BuildError(
                    f"Semantic source {reference}: no source columns are available. Declare its schema or author semantic columns with dataType and sourceColumn."
                )
        observed[reference] = {
            **metadata,
            "reference": reference,
            "object_type": kind,
            "source_columns": columns,
            "description": description,
            "column_notes": notes,
        }
    return observed


def _managed_kind(identity, repository, bindings, catalogue):
    """``table`` or ``view`` for a selected or installed managed relation."""

    if identity.item in bindings.by_item:
        authored = repository.source_documents.get(identity)
        if authored is not None and authored.kind in {TABLE, VIEW}:
            return authored.kind.lower()
    registered = catalogue.registered.get(identity)
    if registered is not None and registered.object_type in {"table", "view"}:
        return registered.object_type
    return None


def _require_managed(
    reference, contribution, table, *, repository, bindings, catalogue, workspace
):
    """Refuse a Weaver.Source that names no managed Table or View.

    Without a catalogue nothing is installed. A project item must declare the
    relation; any other item must at least be a configured or mapped target.
    """

    identity = source_identity(reference)
    if workspace.catalogue:
        if _managed_kind(identity, repository, bindings, catalogue):
            return
        problem = "is not an installed or selected Table or View"
        action = f"Correct the reference, or build {identity.item} first"
    elif identity.item in {item.identity for item in repository.items}:
        document = repository.source_documents.get(identity)
        if document is not None and document.kind in {TABLE, VIEW}:
            return
        problem = f"is not a Table or View that {identity.item} declares"
        action = "Correct the reference"
    elif (
        identity.item in workspace.configured_items
        or str(identity.item) in contribution.expression_sources
    ):
        return
    else:
        problem = (
            f"names {identity.item}, which is not a project item or configured target"
        )
        action = f"Correct the reference, or add a targets: entry for {identity.item}"
    from ..semantic_models.annotation import declared_location

    where = declared_location(
        contribution, (("table", table), ("annotation", "Weaver.Source"))
    )
    raise BuildError(
        f"{where or f'table {table!r}'}: Weaver.Source {reference} {problem}. {action}"
    )

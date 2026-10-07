"""Resolve semantic source state before pure Build planning."""

from ..catalogue.claims import catalogue_columns
from ..declaration.metadata import TABLE, VIEW
from ..declaration.model import WeaverItemId
from ..errors import BuildError
from ..semantic_models.fragments import needs_source_columns, source_table
from ..semantic_models.references import source_identity
from ..targets import physical_item


def read_semantic_sources(repository, bindings, catalogue, *, session, workspace):
    wanted = {}
    mappings = {}
    for item, contribution in repository.semantic_models.items():
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
            wanted[reference] = wanted.get(reference, False) or needs_source_columns(
                tables[table]
            )
    observed = {}
    for reference, needs_columns in sorted(wanted.items()):
        identity = source_identity(reference)
        bound = bindings.by_item.get(identity.item)
        authored = repository.source_documents.get(identity) if bound else None
        registered = catalogue.registered.get(identity)
        if authored is not None and authored.kind in {TABLE, VIEW}:
            kind = authored.kind.lower()
        elif registered is not None and registered.object_type in {"table", "view"}:
            kind = registered.object_type
        else:
            raise BuildError(
                f"Semantic source {reference}: no managed Table or View. Build the source or correct the path."
            )
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
        # A selected inferred source may only acquire its shape during installation.
        if needs_columns and not columns and bound is not None and authored is not None:
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

"""Logical Report certification and model dependency rows."""

from ..declaration.model import WeaverDocumentId
from .claims import catalogue_columns
from .tables import DEPENDENCY, REGISTRY, ROLE_DATA, ROLE_SOURCE


def project_report(item, contribution):
    root = WeaverDocumentId.report_root(item)
    schema, name = catalogue_columns(root)
    common = {"item_type": item.item_type, "item_name": item.item_name}
    return {
        REGISTRY.name: (
            {
                **common,
                "schema_name": schema,
                "object_name": name,
                "object_type": "report",
                "object_role": ROLE_DATA,
                "signature": contribution.signature,
            },
            {
                **common,
                "schema_name": "Source",
                "object_name": name,
                "object_type": "source_artifact",
                "object_role": ROLE_SOURCE,
                "signature": contribution.source_signature,
            },
        ),
        DEPENDENCY.name: (
            {
                **common,
                "referencing_schema_name": schema,
                "referencing_object_name": name,
                "dependency_reference": str(contribution.model),
                "referenced_item_type": contribution.model.item_type,
                "referenced_item_name": contribution.model.item_name,
                "referenced_schema_name": "",
                "referenced_object_name": "",
                "signature": contribution.signature,
            },
        )
        if contribution.model is not None
        else (),
    }

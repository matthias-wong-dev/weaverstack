"""Reconcile one item-scoped catalogue projection."""

from __future__ import annotations

from .item_tables import DICTIONARY_TABLES, INSTALLATION, REGISTRY
from .reconcile import CatalogueReconciliation, TableReconciliation
from .render import render_delete_obsolete, render_merge


def reconcile_item(projection) -> CatalogueReconciliation:
    scope = projection.scope

    def one(table):
        rows = projection.for_table(table)
        return TableReconciliation(
            table=table,
            delete=render_delete_obsolete(table, rows, scope=scope),
            merge=render_merge(table, rows, scope=scope),
        )

    return CatalogueReconciliation(
        scope=scope,
        dictionaries=tuple(one(table) for table in DICTIONARY_TABLES),
        installation=one(INSTALLATION),
        registry=one(REGISTRY),
    )

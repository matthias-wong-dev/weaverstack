"""Recorded catalogue answers for semantic wipe tests."""

from weaver.catalogue.connection import CatalogueConnection
from weaver.catalogue.reader import read_table
from weaver.catalogue.tables import INSTALLATION


def catalogue_answers(rows):
    shape = [
        {
            "TABLE_NAME": INSTALLATION.name,
            "COLUMN_NAME": INSTALLATION.public_name_of(name),
        }
        for name in INSTALLATION.physical_columns
    ]
    asked = []

    def discover(statement):
        asked.append(statement)
        return shape if len(asked) == 1 else []

    read_table(CatalogueConnection(discover), INSTALLATION)
    assert len(asked) == 2
    values = [
        {name: row.get(name) for name in INSTALLATION.column_names} for row in rows
    ]
    return {asked[0]: shape, asked[1]: values}

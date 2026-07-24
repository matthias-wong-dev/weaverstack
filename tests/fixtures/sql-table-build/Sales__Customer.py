"""
Table ID: Sales.Customer

Description: One row per customer — the base table the SQL tables read.

Lineage: A deterministic base for the SQL-table build fixture.

Primary key: CustomerId

Schema:
  CustomerId: integer
  CustomerName: string
"""

from weaver import Table


class Sales__Customer(Table):
    def read(self):
        # Build must never call read(): it creates structure, not data
        # (build-philosophy §1, §15). If a build ever loaded, this would prove
        # it by failing loudly rather than silently reading rows.
        raise RuntimeError("read() must not run during build")

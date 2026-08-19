"""
Table ID: Sales.Customer

Description: One row per customer, read from the delivered extract.

Lineage: $Files/Sales.Seed

Primary key: Customer id

Schema:
  Customer id: string
  Customer name: string
"""

from Files.Sales__Seed import Sales__Seed

from weaver import Table


class Sales__Customer(Table):
    def read(self):
        source = Sales__Seed(self).spark_path()
        rows = self.spark.read.csv(source, header=True)
        # No explicit deletes: the extract is the whole truth, so a row's absence
        # from it is what retires it.
        return rows

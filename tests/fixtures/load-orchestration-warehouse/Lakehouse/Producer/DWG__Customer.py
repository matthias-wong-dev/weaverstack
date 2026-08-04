"""
Table ID: DWG.Customer

Description: One row per customer. The producer side of the cross-item alias.

Lineage: $Files/DWG.Seed

Primary key: CustomerId

Schema:
  CustomerId: integer
  CustomerName: string
"""

from Files.DWG__Seed import DWG__Seed

from weaver import Table


class DWG__Customer(Table):
    def read(self):
        # Joined as text: `path()` is the `abfss://` URL Spark reads, and
        # `pathlib` cannot parse one — it collapses the scheme's slashes.
        source = f"{DWG__Seed(self).path()}/customers.csv"
        raw = self.spark.read.csv(source, header=True, inferSchema=False)
        shaped = raw.selectExpr(
            "cast(CustomerId as int) as CustomerId",
            "CustomerName as CustomerName",
        )
        return shaped, None

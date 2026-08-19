"""
Table ID: DWG.Customer

Description: One row per customer, typed from the raw CSV.

Lineage: $Files/Raw.CustomerCsv

Primary key: CustomerId

Schema:
  CustomerId: integer
  CustomerName: string
  IsActive: boolean

Revision notes:
  - 2026-07-24 Created.
"""

# The deployed tree reproduces the authored layout beneath the runtime root, so
# a Files document is imported as `Files.<module>` once that root is on the path.
from Files.Raw__CustomerCsv import Raw__CustomerCsv

from weaver import Table


class DWG__Customer(Table):
    def read(self):
        # `spark_path()`, because Spark is what reads it: in Fabric that is an
        # `abfss://` URL, which `pathlib` collapses to `abfss:/` and rebuilds
        # into a location that does not exist. `path()` is the mounted Path for
        # ordinary Python, and Spark cannot use one.
        source = f"{Raw__CustomerCsv(self).spark_path()}/customers.csv"
        raw = self.spark.read.csv(source, header=True, inferSchema=False)
        shaped = raw.selectExpr(
            "cast(CustomerId as int) as CustomerId",
            "CustomerName as CustomerName",
            "cast(IsActive as boolean) as IsActive",
        )
        # No explicit deletes: the drop is the whole truth, so absence from it is
        # what retires a row.
        return shaped

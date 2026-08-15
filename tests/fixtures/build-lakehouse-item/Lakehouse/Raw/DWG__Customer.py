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

from weaver import Table

from .Files.Raw__CustomerCsv import Raw__CustomerCsv


class DWG__Customer(Table):
    def read(self):
        # spark_path(), because Spark is what reads it. path() is the mounted
        # Path an ordinary Python open() wants, and Spark cannot use one.
        source = f"{Raw__CustomerCsv(self).spark_path()}/customers.csv"
        raw = self.spark.read.csv(source, header=True, inferSchema=False)
        shaped = raw.selectExpr(
            "cast(CustomerId as int) as CustomerId",
            "CustomerName as CustomerName",
            "cast(IsActive as boolean) as IsActive",
        )
        return shaped, []

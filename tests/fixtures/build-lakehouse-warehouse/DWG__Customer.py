"""
Table ID: DWG.Customer

Description: One row per customer, typed from the raw CSV.

Lineage: $Raw.CustomerCsv

Primary key: CustomerId

Schema:
  CustomerId: integer
  CustomerName: string
  IsActive: boolean

Revision notes:
  - 2026-07-24 Created.
"""

from pathlib import Path

from Raw__CustomerCsv import Raw__CustomerCsv

from weaver import Table


class DWG__Customer(Table):
    def read(self):
        source = Path(Raw__CustomerCsv.folder_path()) / "customers.csv"
        raw = self.spark.read.csv(str(source), header=True, inferSchema=False)
        shaped = raw.selectExpr(
            "cast(CustomerId as int) as CustomerId",
            "CustomerName as CustomerName",
            "cast(IsActive as boolean) as IsActive",
        )
        return shaped, []

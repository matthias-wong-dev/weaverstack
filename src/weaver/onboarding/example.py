"""Generate a Sales example for the project's chosen items.

A Lakehouse receives an export folder, a Delta table and an Assumption. A
Warehouse receives reference regions, a joined customer table and an
Assumption. With both, the Warehouse reads the Lakehouse's customers through a
logical shortcut and does not seed another copy.

`Sales` is the business schema and is implied by the objects themselves, so no
schema document is written. The Fabric item names are the ones the user chose.
"""

from __future__ import annotations

from ..declaration.model import LAKEHOUSE, WAREHOUSE
from .project import ProjectRequest

SCHEMA = "Sales"

_CUSTOMERS_CSV = """\
Customer id,Customer name,Region code
C001,Harbour Freight Pty Ltd,NSW
C002,Riverside Grocers,VIC
C003,Tablelands Coffee Roasters,QLD
C004,Coastal Marine Supplies,NSW
"""

_FOLDER = '''\
"""
Folder ID: Sales.Customers

Description: Customer records as the sales system exports them.

Lineage: Stands in for the nightly export from the sales system.

File key: "*.csv"

Incremental: false

Notes: |
  This example writes its own export, so it loads without an external source.
"""

from weaver import Folder

CUSTOMERS = """\\
{csv}"""


class Sales__Customers(Folder):
    def read(self):
        # Weaver supplies an empty staging directory before read() runs. This
        # method writes the export there and returns the directory.
        with self.staging_folder() as staging:
            (staging.path / "customers.csv").write_text(CUSTOMERS, encoding="utf-8")
        return staging, []
'''

_LAKEHOUSE_TABLE = '''\
"""
Table ID: Sales.Customer

Description: One row per customer in the sales system.

Lineage: $Files/Sales.Customers

Primary key: Customer id

Comparison columns: Customer name, Region code

Schema:
  Customer id: string
  Customer name: string
  Region code: string

Notes: |
  The retained export files can rebuild this table.
"""

from Files.Sales__Customers import Sales__Customers

from weaver import Table


class Sales__Customer(Table):
    def read(self):
        # spark_path() is the abfss:// form Spark reads. path() is the mounted
        # pathlib.Path for ordinary Python.
        exported = Sales__Customers(self).spark_path()
        return (
            self.spark.read.option("header", True)
            .csv(exported)
            .selectExpr("`Customer id`", "`Customer name`", "`Region code`")
            .dropDuplicates(["Customer id"])
        )
'''

_LAKEHOUSE_ASSUMPTION = '''\
"""
Assumption ID: Sales.CustomerValid

Description: Every customer carries a name and a region.

Notes: |
  An Assumption returns rows that contradict its statement. An empty result
  means the Assumption holds.
"""

from Tables.Sales__Customer import Sales__Customer

from weaver import Assumption


class Sales__CustomerValid(Assumption):
    def read(self):
        # Constructing the imported dependency from `self` preserves the
        # Lakehouse selected for this Assumption.
        customers = Sales__Customer(self).dataframe()
        return customers.where(
            "`Customer name` is null or `Region code` is null"
        ).select("Customer id", "Customer name", "Region code")
'''

_WAREHOUSE_REGION = """\
/*
Table ID: Sales.Region

Description: The regions customers are grouped into.

Lineage: Reference data, maintained in this project.

Primary key: Region code

Comparison columns: Region name

Schema:
  Region code: varchar(10)
  Region name: varchar(100)
*/

select v.[Region code]
     , v.[Region name]
  from (values ('NSW', 'New South Wales')
             , ('QLD', 'Queensland')
             , ('VIC', 'Victoria')
       ) as v ([Region code], [Region name]);
"""

_WAREHOUSE_CUSTOMER = """\
/*
Table ID: Sales.Customer

Description: One row per customer in the sales system.

Lineage: Reference data, maintained in this project.

Primary key: Customer id

Comparison columns: Customer name, Region code

Schema:
  Customer id: varchar(50)
  Customer name: varchar(200)
  Region code: varchar(10)
*/

select v.[Customer id]
     , v.[Customer name]
     , v.[Region code]
  from (values ('C001', 'Harbour Freight Pty Ltd', 'NSW')
             , ('C002', 'Riverside Grocers', 'VIC')
             , ('C003', 'Tablelands Coffee Roasters', 'QLD')
             , ('C004', 'Coastal Marine Supplies', 'NSW')
       ) as v ([Customer id], [Customer name], [Region code]);
"""

_WAREHOUSE_JOIN = """\
/*
Table ID: Sales.CustomerByRegion

Description: Customers with the name of the region they belong to.

Lineage: $Sales.Customer

Primary key: Customer id

Comparison columns: Customer name, Region name

Schema:
  Customer id: varchar(50)
  Customer name: varchar(200)
  Region name: varchar(100)

Notes: |
  Query dependencies make this table load after Customer and Region.
*/

select c.[Customer id]
     , c.[Customer name]
     , r.[Region name]
  from [Sales].[Customer] c
  join [Sales].[Region] r on r.[Region code] = c.[Region code];
"""

_WAREHOUSE_ASSUMPTION = """\
/*
Assumption ID: Sales.CustomerByRegionValid

Description: Every customer landed in a region the estate declares.

Notes: |
  A T-SQL validation compiles to a procedure anyone can run without Weaver:

      exec [_].[Assumption Sales.CustomerByRegionValid];
*/

select [Customer id]
     , [Customer name]
  from [Sales].[CustomerByRegion]
 where [Region name] is null;
"""

_SHORTCUTS = """\
# A Warehouse reads a Lakehouse table through its SQL endpoint, not its Delta
# files. The logical shortcut keeps physical Fabric item names out of the query.
logical:
  {warehouse_item}/{schema}.Customer: {lakehouse_item}/Tables/{schema}.Customer
"""


def example_files(request: ProjectRequest) -> dict[str, str]:
    files: dict[str, str] = {}
    if request.lakehouse:
        item = f"{LAKEHOUSE}/{request.lakehouse}"
        files[f"{item}/Files/Sales__Customers.py"] = _FOLDER.format(csv=_CUSTOMERS_CSV)
        files[f"{item}/Tables/Sales__Customer.py"] = _LAKEHOUSE_TABLE
        files[f"{item}/assumptions/Sales__CustomerValid.py"] = _LAKEHOUSE_ASSUMPTION
    if request.warehouse:
        item = f"{WAREHOUSE}/{request.warehouse}"
        files[f"{item}/Sales.Region.sql"] = _WAREHOUSE_REGION
        files[f"{item}/Sales.CustomerByRegion.sql"] = _WAREHOUSE_JOIN
        files[f"{item}/assumptions/Sales.CustomerByRegionValid.sql"] = (
            _WAREHOUSE_ASSUMPTION
        )
        if request.lakehouse:
            # The shortcut replaces a second seeded copy of the customers.
            files[f"{item}/shortcuts.yml"] = _SHORTCUTS.format(
                warehouse_item=item,
                lakehouse_item=f"{LAKEHOUSE}/{request.lakehouse}",
                schema=SCHEMA,
            )
        else:
            files[f"{item}/Sales.Customer.sql"] = _WAREHOUSE_CUSTOMER
    return files

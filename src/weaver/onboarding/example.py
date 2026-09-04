"""A small Sales example, generated into whichever items the project chose.

Three shapes, from the same parts. A Lakehouse holds a folder of exported
customers, a Delta table built from it and an Assumption about that table. A
Warehouse holds reference regions, a table joining customers to them and an
Assumption. With both, the Warehouse reads the Lakehouse's customers through a
logical shortcut, and seeds no customers of its own.

`Sales` is the business schema and is implied by the objects themselves, so no
schema document is written. The Fabric item names are the ones the user chose.
"""

from __future__ import annotations

from ..declaration.model import LAKEHOUSE, WAREHOUSE
from .project import ProjectRequest

#: The schema every generated object belongs to.
SCHEMA = "Sales"


#: Generated documents carry no `Revision notes:`. The key records what changed
#: and when; a generated file has no history yet, and the same request has to
#: produce the same bytes on any day, which is what lets a rerun converge.


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
  A real deployment fetches the export here, over SFTP or from an API. This one
  writes the file itself, so the project loads with nothing set up beside it.
"""

from weaver import Folder

#: The export this example stands in for.
CUSTOMERS = """\\
{csv}"""


class Sales__Customers(Folder):
    def read(self):
        # Weaver issues the staging directory and empties it before read() runs,
        # so a folder fills what it was given and returns it.
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
  Read from the export folder, so the table can be rebuilt from files that were
  kept.
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
  An Assumption states something about the data on its own, and the rows it
  returns are what contradict the statement. Holding looks like an empty result.
"""

from Tables.Sales__Customer import Sales__Customer

from weaver import Assumption


class Sales__CustomerValid(Assumption):
    def read(self):
        # The dependency is the import, constructed from `self` so it resolves
        # against the Lakehouse this Assumption was pointed at.
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
  Weaver reads the dependencies out of the query, so this table loads after both
  the customers and the regions it selects from.
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
# A Lakehouse table, made addressable inside the Warehouse.
#
# The Warehouse cannot read Delta files; it reads the Lakehouse's SQL endpoint.
# Declaring the shortcut lets a Warehouse object select from a Lakehouse object
# without either item knowing where the other is built.
logical:
  {warehouse_item}/{schema}.Customer: {lakehouse_item}/Tables/{schema}.Customer
"""


def example_files(request: ProjectRequest) -> dict[str, str]:
    """The Sales example for this project, as relative path to text."""

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
            # The customers come from the Lakehouse through a shortcut, so the
            # Warehouse does not seed a second copy of them.
            files[f"{item}/shortcuts.yml"] = _SHORTCUTS.format(
                warehouse_item=item,
                lakehouse_item=f"{LAKEHOUSE}/{request.lakehouse}",
                schema=SCHEMA,
            )
        else:
            files[f"{item}/Sales.Customer.sql"] = _WAREHOUSE_CUSTOMER
    return files

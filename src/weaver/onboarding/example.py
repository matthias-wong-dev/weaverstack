"""Small independent pipelines, with additional cross-item declarations."""

from __future__ import annotations

from ..declaration.model import LAKEHOUSE, WAREHOUSE
from .powerbi_example import managed_model_files, powerbi_example_files
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


def example_files(request: ProjectRequest) -> dict[str, str]:
    if request.example == "Parcel":
        return parcel_files(request)
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
        files[f"{item}/Sales.Customer.sql"] = _WAREHOUSE_CUSTOMER
        if request.lakehouse:
            files[f"{item}/shortcuts.yml"] = (
                "logical:\n"
                f"  {item}/Sales.LakehouseCustomer: Lakehouse/{request.lakehouse}/Tables/Sales.Customer\n"
            )
            files[f"{item}/Sales.LakehouseCustomerByRegion.sql"] = (
                _WAREHOUSE_JOIN.replace(
                    "Sales.CustomerByRegion", "Sales.LakehouseCustomerByRegion"
                )
                .replace("[Sales].[Customer] c", "[Sales].[LakehouseCustomer] c")
                .replace(
                    "Lineage: $Sales.Customer", "Lineage: $Sales.LakehouseCustomer"
                )
                .replace(
                    "load after Customer and Region",
                    "load after the Lakehouse customers and Region",
                )
            )
    files.update(powerbi_example_files(request))
    files.update(managed_model_files(request))
    return files


def parcel_files(request: ProjectRequest) -> dict[str, str]:
    from importlib.resources import files as resources

    root = resources("weaver").joinpath("fragments/examples/Parcel")
    result = {}

    def read_tree(folder, prefix):
        for entry in sorted(folder.iterdir(), key=lambda p: p.name):
            path = f"{prefix}/{entry.name}"
            if entry.is_dir():
                read_tree(entry, path)
            else:
                result[path] = entry.read_text(encoding="utf-8")

    for kind, original, selected in (
        (LAKEHOUSE, "Landing", request.lakehouse),
        (WAREHOUSE, "Operations", request.warehouse),
        ("PowerBI", "Parcel", request.example_model),
    ):
        if selected:
            read_tree(root.joinpath(kind, original), f"{kind}/{selected}")
    if request.example_model and request.example_model != "Parcel":
        result = {
            (
                p.replace("/Parcel.", f"/{request.example_model}.")
                if p.startswith("PowerBI/")
                else p
            ): v.replace(
                "Parcel.SemanticModel", f"{request.example_model}.SemanticModel"
            ).replace("Parcel.Report", f"{request.example_model}.Report")
            for p, v in result.items()
        }
    if request.lakehouse and request.warehouse:
        item = f"Warehouse/{request.warehouse}"
        result[f"{item}/shortcuts.yml"] = (
            "logical:\n"
            f"  {item}/Parcel.LakehouseEvent: Lakehouse/{request.lakehouse}/Tables/Parcel.Event\n"
        )
        view = root.joinpath("Warehouse/Operations/Parcel.CurrentStatus.sql").read_text(
            encoding="utf-8"
        )
        result[f"{item}/Parcel.LakehouseCurrentStatus.sql"] = view.replace(
            "View ID: Parcel.CurrentStatus",
            "Table ID: Parcel.LakehouseCurrentStatus\n\nPrimary key: Parcel ID\n\nSchema:\n  Parcel ID: varchar(20)\n  Status: varchar(30)\n  Depot: varchar(50)",
        ).replace("[Parcel].[StatusEvent]", "[Parcel].[LakehouseEvent]")
    result.update(managed_model_files(request))
    return result

# Weaver Example

This example demonstrates that the **same Weaver repository** can be
built in two different ways.

Both modes produce exactly the same Fabric estate.

    Weaver
    Sales
    Reporting

The repository is authored once and stored inside the notebook's
built-in resources:

``` text
Sales-Estate.Notebook/
└── Resources/
    └── builtin/
        └── repository/
```

The difference is simply **where `weaver.build()` executes**.

------------------------------------------------------------------------

# Notebook mode

In Notebook mode, everything runs inside Microsoft Fabric.

The notebook reads the repository directly from its built-in resources.

``` python
from pathlib import Path
import weaver

repository = Path.cwd() / "repository"

weaver.wipe(
    [
        "Lakehouse/Sales",
        "Warehouse/Reporting",
        "Warehouse/Weaver",
    ]
)

result = weaver.build(
    repository,
    bind=[
        "Lakehouse/Sales=Sales",
        "Warehouse/Reporting=Reporting",
    ],
)

assert result.succeeded
```

Inside Fabric, Weaver automatically discovers:

-   the current workspace
-   the configured Weaver catalogue

No workspace configuration file is required.

Notebook mode is intended for:

-   interactive development
-   demonstrations
-   building directly inside Fabric

------------------------------------------------------------------------

# Desktop mode

Exactly the same repository can be built from a desktop checkout.

The CLI simply points at the exported notebook resources.

``` text
examples/
├── weaver_example.yml
└── workspaces/
    └── Weaver Example/
        └── Sales-Estate.Notebook/
            └── Resources/
                └── builtin/
                    └── repository/
```

The destination workspace is described by:

``` text
weaver_example.yml
```

Example:

``` bash
weaver build \
  "examples/workspaces/Weaver Example/Sales-Estate.Notebook/Resources/builtin/repository" \
  --workspace-config "examples/weaver_example.yml"
```

Desktop mode parses the repository locally, computes the build plan,
uploads the build bundle, and executes it in Fabric.

The resulting Fabric estate is identical to Notebook mode.

## The same lifecycle from Python

The CLI is a thin adapter over the Python API, so a desktop script does what
`weaver compose dev` does. [`lifecycle.py`](lifecycle.py) is that script — wipe,
build, load and test, through one Session:

``` bash
python examples/lifecycle.py   --workspace "Weaver Example"   --catalogue Weaver   --environment weaver   --lakehouse Sales   --warehouse Reporting
```

One Session is opened and passed to each operation, which is what makes this
worth writing down: the credential, the resolved items, the Livy session and the
Warehouse connections are paid for once rather than four times.

------------------------------------------------------------------------

# Loading the estate

A build creates structure; a load puts rows in it. The Sales Lakehouse
carries one of each primitive Weaver installs, and all four are loaded
the same way — by importing the deployed module and calling `.load()`:

``` python
from Files.Sales__OrderExport import Sales__OrderExport  # a Python folder
from Sales__Customer import Sales__Customer  # a Python table
from Sales__Order import Sales__Order  # a Python table
from Sales__OrderSummary import Sales__OrderSummary  # a Spark SQL table

catalogue = "Warehouse/Weaver"

Sales__OrderExport(spark, catalogue=catalogue).load()
Sales__Customer(spark, catalogue=catalogue).load()
Sales__Order(spark, catalogue=catalogue).load()
Sales__OrderSummary(spark, catalogue=catalogue).load()
```

Naming the catalogue makes each object *catalogue-anchored*: it has a place in
the estate's own record of itself, so a clean load advances its bookmark.
`Sales__Customer(spark)` would be freestanding, which is for reading: `read()`
runs, and `load()` refuses, because a load records how far it read. A constructor
argument rather than a `load()` one, because an authored `read()` is called by
Weaver and takes nothing, so whatever it may reach is set before the load
begins.

`Sales.OrderSummary` is authored as `Sales.OrderSummary.sql` and
installed as `Sales__OrderSummary.py` — a `SparkSqlTable` carrying the
authored SQL. Nothing about loading it differs, which is the point: the
whole Delta load lifecycle lives in one place, so a table authored in SQL
and one authored in Python cannot come to behave differently.

Or orchestrate the lot, in dependency order, from either mode:

``` bash
weaver load Lakehouse/Sales Warehouse/Reporting \
  --workspace-config "examples/weaver_example.yml"
```

------------------------------------------------------------------------

# Reading only what has arrived

`Sales.Order` is incremental, and it reads incrementally. `self.bookmark()` is the
UTC instant immediately before its most recent clean load began, and the folder
records when each of its files changed, so the two compose into "what has
arrived since":

``` python
class Sales__Order(Table):
    def read(self):
        export = Sales__OrderExport(self)
        arrived = export.files_since(self.bookmark())
        if not arrived:
            return self.empty_dataframe(), None
        ...
```

A table that has never loaded cleanly carries the sentinel,
`1900-01-01T00:00:00Z`, and every file is newer than that — so the first load
reads the lot and later ones read the night's delivery. Watch it in the load
report: `Sales.Customer` reads the whole folder every time because it is the
whole truth about its customers, and `Sales.Order` reads only what is new.

``` text
first load, one export file
  load:Lakehouse/Sales/Sales.Customer  (read 3, +3 ~0 -0 !0)
  load:Lakehouse/Sales/Sales.Order     (read 4, +4 ~0 -0 !0)

again, nothing delivered
  load:Lakehouse/Sales/Sales.Customer  (read 3, +0 ~0 -0 !0)
  load:Lakehouse/Sales/Sales.Order     (read 0, +0 ~0 -0 !0)

a second night's file arrives
  load:Lakehouse/Sales/Sales.Customer  (read 4, +1 ~0 -0 !0)
  load:Lakehouse/Sales/Sales.Order     (read 2, +2 ~0 -1 !0)
```

The `-1` is an order the new file marked cancelled. Absence would not have
retired it: an incremental source is a window, so a row missing from tonight's
file is older than the window rather than gone.

One caveat worth knowing: `files_since` refuses a folder that holds managed
files with no change history, because Weaver never saw those files arrive and
cannot say when they changed. A folder load that changes nothing appends nothing,
so a folder whose history was lost stays without one until it next publishes.

------------------------------------------------------------------------

# Two spellings of one folder

A `Folder` is reached two ways, because two things read it and neither
understands the other's spelling:

``` python
folder.path()  # pathlib.Path — open(), glob(), write_text()
folder.spark_path()  # str — spark.read, and abfss:// on Fabric
```

`Sales__OrderExport` uses the first to read its own files; `Sales__Order`
and `Sales__Customer` use the second to hand the folder to Spark.

------------------------------------------------------------------------

# Why two modes?

Notebook mode is convenient when authoring directly inside Fabric.

Desktop mode is convenient when using local editors, Git workflows,
CI/CD, or AI coding agents.

The important architectural idea is that the **repository is portable**.

The repository is authored once.

It can be executed from a Fabric notebook or from the desktop CLI
without modification.

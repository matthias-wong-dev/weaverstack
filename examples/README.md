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

Sales__OrderExport(spark).load()
Sales__Customer(spark).load()
Sales__Order(spark).load()
Sales__OrderSummary(spark).load()
```

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

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

weaver.wipe([
    "Lakehouse/Sales",
    "Warehouse/Reporting",
    "Lakehouse/Weaver",
])

result = weaver.build(
    repository,
    bind=[
        "Lakehouse/Sales=Lakehouse/Sales",
        "Warehouse/Reporting=Warehouse/Reporting",
    ],
)

assert result.succeeded
```

Inside Fabric, Weaver automatically discovers:

-   the current workspace
-   the attached Weaver control Lakehouse

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
└── Weaver Example/
    ├── weaver_example.yml
    └── workspace/
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

------------------------------------------------------------------------

# Why two modes?

Notebook mode is convenient when authoring directly inside Fabric.

Desktop mode is convenient when using local editors, Git workflows,
CI/CD, or AI coding agents.

The important architectural idea is that the **repository is portable**.

The repository is authored once.

It can be executed from a Fabric notebook or from the desktop CLI
without modification.

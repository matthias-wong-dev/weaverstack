from weaver import Shortcut

#: What Landing materialised, presented in this item's own namespace. Logical, so
#: Weaver follows Landing's binding rather than naming a physical Lakehouse.
SRC__Customer = Shortcut(
    shortcut_type="table",
    target_type="logical",
    target="Lakehouse/Landing/Tables/LAND.Customer",
)

SRC__Transaction = Shortcut(
    shortcut_type="table",
    target_type="logical",
    target="Lakehouse/Landing/Tables/LAND.Transaction",
)

SRC__Product = Shortcut(
    shortcut_type="table",
    target_type="logical",
    target="Lakehouse/Landing/Tables/LAND.Product",
)

SRC__Region = Shortcut(
    shortcut_type="table",
    target_type="logical",
    target="Lakehouse/Landing/Tables/LAND.Region",
)

SRC__SourceEvents = Shortcut(
    shortcut_type="folder",
    target_type="logical",
    target="Lakehouse/Landing/Files/LAND.SourceEvents",
)

SRC__GeneratedEvents = Shortcut(
    shortcut_type="folder",
    target_type="logical",
    target="Lakehouse/Landing/Files/LAND.GeneratedEvents",
)

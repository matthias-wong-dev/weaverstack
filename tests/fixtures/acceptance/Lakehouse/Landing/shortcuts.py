from weaver import Shortcut

#: The foreign Lakehouse's mutable Delta table.
Source__Customer = Shortcut(
    shortcut_type="table",
    target_type="physical",
    target="Lakehouse/{{EXTERNAL_LAKEHOUSE}}/Source.Customer",
    workspace="{{EXTERNAL_WORKSPACE}}",
)

#: The foreign Lakehouse's stable product table.
Source__Product = Shortcut(
    shortcut_type="table",
    target_type="physical",
    target="Lakehouse/{{EXTERNAL_LAKEHOUSE}}/Reference.Product",
    workspace="{{EXTERNAL_WORKSPACE}}",
)

#: The same foreign namespace, presented whole. Landing declares no schema of
#: this name and owns nothing in it.
#:
#: Declared but not read by a load. A build waits until a table shortcut it
#: created is readable and deliberately does not wait for a schema shortcut, so
#: a load in the same run can reach one before Fabric has discovered it.
Reference = Shortcut(
    shortcut_type="schema",
    target_type="physical",
    target="Lakehouse/{{EXTERNAL_LAKEHOUSE}}/Reference",
    workspace="{{EXTERNAL_WORKSPACE}}",
)

#: The foreign Lakehouse's mutable file drop.
Source__Events = Shortcut(
    shortcut_type="folder",
    target_type="physical",
    target="Lakehouse/{{EXTERNAL_LAKEHOUSE}}/Files/Source/Events",
    workspace="{{EXTERNAL_WORKSPACE}}",
)

#: The foreign Warehouse's mutable table, read through its OneLake publication.
Source__Transaction = Shortcut(
    shortcut_type="table",
    target_type="physical",
    target="Warehouse/{{EXTERNAL_WAREHOUSE}}/Source.Transaction",
    workspace="{{EXTERNAL_WORKSPACE}}",
)

#: The foreign Warehouse's stable table.
Source__Region = Shortcut(
    shortcut_type="table",
    target_type="physical",
    target="Warehouse/{{EXTERNAL_WAREHOUSE}}/Reference.Region",
    workspace="{{EXTERNAL_WORKSPACE}}",
)

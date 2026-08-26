from weaver import Shortcut

#: The foreign Lakehouse's mutable Delta table.
Source__Customer = Shortcut(
    shortcut_type="table",
    target_type="physical",
    target="Lakehouse/{{EXTERNAL_LAKEHOUSE}}/Source.Customer",
    workspace="{{EXTERNAL_WORKSPACE}}",
)

#: The same foreign namespace, presented whole. Landing declares no schema of
#: this name and owns nothing in it.
#:
#: ``LAND.Product`` reads its tables, so a load exercises the runtime
#: schema-shortcut API.
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

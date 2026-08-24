from weaver import Shortcut

#: What Serving materialised. Physical rather than logical: a Lakehouse shortcut
#: is a OneLake shortcut, and Weaver has no logical form for a Warehouse source,
#: so this names the Warehouse the estate binds Serving to.
WH__Reporting = Shortcut(
    shortcut_type="table",
    target_type="physical",
    target="Warehouse/{{SERVING_WAREHOUSE}}/SERVE.Reporting",
)

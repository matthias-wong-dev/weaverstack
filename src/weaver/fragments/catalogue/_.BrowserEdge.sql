/*
Table ID: _.BrowserEdge

Description: >-
  One row per edge of the installed graph, scoped to the downstream node's
  item. Both ends are BrowserNode IDs, except that an External edge's
  upstream is the reference as its author wrote it.

Lineage: >-
  Projected from the installed graph by Weaver's own build, and maintained
  only by the catalogue DML a build appends. Never populated by a load.

Dependencies: []

Static: true

Prohibit rebuild: true

Has load procedure: false

Primary key: Item type, Item name, Downstream node ID, Upstream node ID, Edge kind

Not null:
  - Signature

Schema:
  Item type: varchar(128)
  Item name: varchar(128)
  Downstream node ID: varchar(1000)
  Upstream node ID: varchar(1000)
  Edge kind: varchar(128)
  Through node ID: varchar(1000)
  Signature: varchar(128)

Column notes:
  Item type: >-
    Logical Weaver item type.
  Item name: >-
    Logical Weaver item name.
  Downstream node ID: >-
    The node that reads.
  Upstream node ID: >-
    The node read. For an External edge, the reference as its author wrote
    it.
  Edge kind: >-
    Dependency, Shortcut, Validation or External.
  Through node ID: >-
    The shortcut the read passed through, if any.
  Signature: >-
    Content hash of the edge's row, so a change can be detected.
*/
select cast(null as varchar(128)) as [Item type]
     , cast(null as varchar(128)) as [Item name]
     , cast(null as varchar(1000)) as [Downstream node ID]
     , cast(null as varchar(1000)) as [Upstream node ID]
     , cast(null as varchar(128)) as [Edge kind]
     , cast(null as varchar(1000)) as [Through node ID]
     , cast(null as varchar(128)) as [Signature]
 where 1 = 0

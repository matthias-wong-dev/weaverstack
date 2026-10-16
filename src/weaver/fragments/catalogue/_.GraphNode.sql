/*
Table ID: _.GraphNode

Description: >-
  One row per node of the installed graph, scoped to its item: data objects,
  shortcut destinations, Tests, Assumptions, semantic models and Reports. A
  report reads it with GraphEdge to draw lineage without resolving Weaver
  references.

Lineage: >-
  Projected from the installed graph by Weaver's own build, and maintained
  only by the catalogue DML a build appends. Never populated by a load.

Dependencies: []

Static: true

Prohibit rebuild: true

Has load procedure: false

Primary key: Item type, Item name, Schema name, Object name

Not null:
  - Node ID
  - Node key
  - Node kind
  - Label
  - Item label
  - Search text
  - Is internal
  - Can load
  - Is static
  - Is mirrored
  - Is installed
  - Signature

Schema:
  Item type: varchar(128)
  Item name: varchar(128)
  Schema name: varchar(128)
  Object name: varchar(128)
  Node ID: varchar(1000)
  Node key: varchar(12)
  Node kind: varchar(128)
  Label: varchar(1000)
  Item label: varchar(1000)
  Description: varchar(4000)
  Search text: varchar(4000)
  Is internal: bit
  Can load: bit
  Is static: bit
  Is mirrored: bit
  Is installed: bit
  Signature: varchar(128)

Column notes:
  Item type: >-
    Logical Weaver item type.
  Item name: >-
    Logical Weaver item name.
  Schema name: >-
    The node's schema, as Registry stores it.
  Object name: >-
    The node's name, as Registry stores it.
  Node ID: >-
    The node's installed identity, as GraphEdge names it.
  Node key: >-
    Twelve hexadecimal digits of the Node ID's SHA-256, as GraphEdge's keys
    name it.
  Node kind: >-
    Table, View, Folder, Shortcut, Test, Assumption, Semantic model or Report.
  Label: >-
    The node's display name.
  Item label: >-
    The node's item, as Type/Name.
  Description: >-
    The declared description, if any.
  Search text: >-
    Label, item and description in lower case, for search.
  Is internal: >-
    Whether the node is part of the Weaver catalogue.
  Can load: >-
    Whether Weaver loads the node in this estate.
  Is static: >-
    Whether the node is loaded once rather than refreshed.
  Is mirrored: >-
    Whether another catalogue's estate supplies the node's data.
  Is installed: >-
    Whether the node's runnable artefact or definition is installed.
  Signature: >-
    Content hash of the node's row, so a change can be detected.
*/
select cast(null as varchar(128)) as [Item type]
     , cast(null as varchar(128)) as [Item name]
     , cast(null as varchar(128)) as [Schema name]
     , cast(null as varchar(128)) as [Object name]
     , cast(null as varchar(1000)) as [Node ID]
     , cast(null as varchar(12)) as [Node key]
     , cast(null as varchar(128)) as [Node kind]
     , cast(null as varchar(1000)) as [Label]
     , cast(null as varchar(1000)) as [Item label]
     , cast(null as varchar(4000)) as [Description]
     , cast(null as varchar(4000)) as [Search text]
     , cast(null as bit) as [Is internal]
     , cast(null as bit) as [Can load]
     , cast(null as bit) as [Is static]
     , cast(null as bit) as [Is mirrored]
     , cast(null as bit) as [Is installed]
     , cast(null as varchar(128)) as [Signature]
 where 1 = 0

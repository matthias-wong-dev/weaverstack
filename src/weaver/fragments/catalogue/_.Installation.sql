/*
Table ID: _.Installation

Description: >-
  One row per logical Item, recording its resolved Fabric workspace and target,
  and the Weaver version and source signature that installed it.

Lineage: >-
  Projected from validated Weaver document declarations by Weaver's own
  build, and maintained only by the catalogue DML a build appends. Never
  populated by a load.

Dependencies: []

Static: true

Prohibit rebuild: true

Has load procedure: false

Primary key: Item type, Item name

Not null:
  - Workspace name
  - Workspace GUID
  - Target name
  - Target item GUID
  - Weaver version
  - Signature

Schema:
  Item type: varchar(128)
  Item name: varchar(128)
  Workspace name: varchar(128)
  Workspace GUID: varchar(128)
  Target name: varchar(128)
  Target item GUID: varchar(128)
  Weaver version: varchar(128)
  Signature: varchar(128)

Column notes:
  Item type: >-
    Logical Weaver item type.
  Item name: >-
    Logical Weaver item name.
  Workspace name: >-
    The resolved Fabric workspace display name.
  Workspace GUID: >-
    The resolved Fabric workspace GUID.
  Target name: >-
    The resolved Fabric target display name.
  Target item GUID: >-
    The resolved Fabric target item GUID.
  Weaver version: >-
    The Weaver version that last reconciled this installation.
  Signature: >-
    Content hash of the Item declaration, so a change can be detected.
*/
select cast(null as varchar(128)) as [Item type]
     , cast(null as varchar(128)) as [Item name]
     , cast(null as varchar(128)) as [Workspace name]
     , cast(null as varchar(128)) as [Workspace GUID]
     , cast(null as varchar(128)) as [Target name]
     , cast(null as varchar(128)) as [Target item GUID]
     , cast(null as varchar(128)) as [Weaver version]
     , cast(null as varchar(128)) as [Signature]
 where 1 = 0

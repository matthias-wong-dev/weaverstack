/*
Table ID: _.Installation

Description: >-
  One row per logical Item, recording its current physical target and the
  Weaver version and source signature that installed it.

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
  - Target name
  - Weaver version
  - Signature

Schema:
  Item type: varchar(128)
  Item name: varchar(128)
  Target name: varchar(128)
  Weaver version: varchar(128)
  Signature: varchar(128)

Column notes:
  Item type: >-
    Logical Weaver item type.
  Item name: >-
    Logical Weaver item name.
  Target name: >-
    The physical item currently bound to this installation.
  Weaver version: >-
    The Weaver version that last reconciled this installation.
  Signature: >-
    Content hash of the Item declaration, so a change can be detected.
*/
select cast(null as varchar(128)) as [Item type]
     , cast(null as varchar(128)) as [Item name]
     , cast(null as varchar(128)) as [Target name]
     , cast(null as varchar(128)) as [Weaver version]
     , cast(null as varchar(128)) as [Signature]
 where 1 = 0

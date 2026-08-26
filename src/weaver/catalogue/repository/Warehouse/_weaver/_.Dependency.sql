/*
Table ID: _.Dependency

Description: >-
  One row per resolved dependency edge, scoped to the referencing item. The
  referenced side is the edge Weaver resolved; the authored spelling is kept
  alongside it. Crossing items or engines is a shortcut, recorded
  separately, not a dependency that changes namespace.

Lineage: >-
  Projected from validated Weaver document declarations by Weaver's own
  build, and maintained only by the catalogue DML a build appends. Never
  populated by a load.

Dependencies: []

Static: true

Prohibit rebuild: true

Has load procedure: false

Primary key: Item type, Item name, Referencing schema name, Referencing object name, Dependency reference

Not null:
  - Signature

Schema:
  Item type: varchar(128)
  Item name: varchar(128)
  Referencing schema name: varchar(128)
  Referencing object name: varchar(128)
  Dependency reference: varchar(1000)
  Referenced item type: varchar(128)
  Referenced item name: varchar(128)
  Referenced schema name: varchar(128)
  Referenced object name: varchar(128)
  Signature: varchar(128)

Column notes:
  Item type: >-
    Logical Weaver item type.
  Item name: >-
    Logical Weaver item name.
  Referencing schema name: >-
    The schema of the object declaring the dependency.
  Referencing object name: >-
    The name of the object declaring the dependency.
  Dependency reference: >-
    The dependency exactly as the owning document wrote it.
  Referenced item type: >-
    The referenced item's type, when the edge resolved.
  Referenced item name: >-
    The referenced item's name, when the edge resolved.
  Referenced schema name: >-
    The referenced schema, when the edge resolved.
  Referenced object name: >-
    The referenced object, when the edge resolved.
  Signature: >-
    Content hash of the owning object's source file, so a change can be
    detected.
*/
select cast(null as varchar(128)) as [Item type]
     , cast(null as varchar(128)) as [Item name]
     , cast(null as varchar(128)) as [Referencing schema name]
     , cast(null as varchar(128)) as [Referencing object name]
     , cast(null as varchar(1000)) as [Dependency reference]
     , cast(null as varchar(128)) as [Referenced item type]
     , cast(null as varchar(128)) as [Referenced item name]
     , cast(null as varchar(128)) as [Referenced schema name]
     , cast(null as varchar(128)) as [Referenced object name]
     , cast(null as varchar(128)) as [Signature]
 where 1 = 0

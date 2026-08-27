/*
Table ID: _.Shortcut

Description: >-
  Every shortcut an item declares, reproduced from its own shortcuts.py or
  shortcuts.yml. This is where the estate's graph crosses items, engines and
  workspaces, so it is kept apart from Dependency: composing Dependency,
  Shortcut and Registry is what yields the whole DAG, and only that
  composition may cross. It records what was declared, so where a logical
  target is physically installed stays Installation's answer.

Lineage: >-
  Projected from validated Weaver document declarations by Weaver's own
  build, and maintained only by the catalogue DML a build appends. Never
  populated by a load.

Dependencies: []

Static: true

Prohibit rebuild: true

Has load procedure: false

Primary key: Item type, Item name, Shortcut ID

Not null:
  - Schema name
  - Shortcut type
  - Target type
  - Target item type
  - Target item name
  - Target schema name
  - Signature

Schema:
  Item type: varchar(128)
  Item name: varchar(128)
  Shortcut ID: varchar(128)
  Schema name: varchar(128)
  Object name: varchar(128)
  Shortcut type: varchar(128)
  Target type: varchar(128)
  Target item type: varchar(128)
  Target item name: varchar(128)
  Target schema name: varchar(128)
  Target object name: varchar(128)
  Target workspace name: varchar(128)
  Signature: varchar(128)

Column notes:
  Item type: >-
    Logical Weaver item type.
  Item name: >-
    Logical Weaver item name.
  Shortcut ID: >-
    The shortcut as its author declared it: 'Sales.Customer' for a table or
    folder, 'Reference' for a schema.
  Schema name: >-
    The schema this item presents the shortcut in.
  Object name: >-
    The object this item presents. Null for a schema shortcut, which
    presents a namespace rather than an object.
  Shortcut type: >-
    What the shortcut is.
  Target type: >-
    How the target is read: a Weaver item Weaver binds, or the Fabric item
    itself.
  Target item type: >-
    The target's item type.
  Target item name: >-
    The target's item name.
  Target schema name: >-
    The schema or path the target sits in.
  Target object name: >-
    The object the target names. Null where it names a schema or a path
    rather than an object. For a logical target these four target columns
    give the producer's identity whole, so nothing rebuilds it without
    joining Installation or splitting an id.
  Target workspace name: >-
    The workspace the target is in. Null for a logical target, which is
    bound, and for a physical one in this workspace.
  Signature: >-
    Content hash of the shortcut declaration, so a change can be detected.
*/
select cast(null as varchar(128)) as [Item type]
     , cast(null as varchar(128)) as [Item name]
     , cast(null as varchar(128)) as [Shortcut ID]
     , cast(null as varchar(128)) as [Schema name]
     , cast(null as varchar(128)) as [Object name]
     , cast(null as varchar(128)) as [Shortcut type]
     , cast(null as varchar(128)) as [Target type]
     , cast(null as varchar(128)) as [Target item type]
     , cast(null as varchar(128)) as [Target item name]
     , cast(null as varchar(128)) as [Target schema name]
     , cast(null as varchar(128)) as [Target object name]
     , cast(null as varchar(128)) as [Target workspace name]
     , cast(null as varchar(128)) as [Signature]
 where 1 = 0

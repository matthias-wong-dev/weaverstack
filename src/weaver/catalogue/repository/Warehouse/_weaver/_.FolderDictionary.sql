/*
Table ID: _.FolderDictionary

Description: >-
  Managed folders. A folder keeps its two-part Weaver document identity
  rather than being reduced to a path, and its file key is the scope of what
  Weaver manages inside it, so reconciliation deletes nothing outside that.

Lineage: >-
  Projected from validated Weaver document declarations by Weaver's own
  build, and maintained only by the catalogue DML a build appends. Never
  populated by a load.

Dependencies: []

Static: true

Prohibit rebuild: true

Has load procedure: false

Primary key: Item type, Item name, Schema name, Object name

Not null:
  - Signature

Schema:
  Item type: varchar(128)
  Item name: varchar(128)
  Schema name: varchar(128)
  Object name: varchar(128)
  Description: varchar(4000)
  Description reference: varchar(128)
  Lineage: varchar(4000)
  Lineage reference: varchar(128)
  File key: varchar(1000)
  Is incremental: bit
  Is static: bit
  Prohibit rebuild: bit
  Signature: varchar(128)

Column notes:
  Item type: >-
    Logical Weaver item type.
  Item name: >-
    Logical Weaver item name.
  Schema name: >-
    The object's schema.
  Object name: >-
    The object's name.
  Description: >-
    What this folder is.
  Description reference: >-
    The $$Schema.Object the description was copied from, when it was
    declared as a reference rather than written here.
  Lineage: >-
    Where this object's data comes from.
  Lineage reference: >-
    The $$Schema.Object the lineage was copied from, if any.
  File key: >-
    The glob patterns Weaver manages, in declared order.
  Is incremental: >-
    Whether load accumulates rows rather than replacing them.
  Is static: >-
    Whether the object is loaded once rather than refreshed.
  Prohibit rebuild: >-
    Whether build may drop and recreate this object.
  Signature: >-
    Content hash of the object's source file, so a change can be detected.
*/
select cast(null as varchar(128)) as [Item type]
     , cast(null as varchar(128)) as [Item name]
     , cast(null as varchar(128)) as [Schema name]
     , cast(null as varchar(128)) as [Object name]
     , cast(null as varchar(4000)) as [Description]
     , cast(null as varchar(128)) as [Description reference]
     , cast(null as varchar(4000)) as [Lineage]
     , cast(null as varchar(128)) as [Lineage reference]
     , cast(null as varchar(1000)) as [File key]
     , cast(null as bit) as [Is incremental]
     , cast(null as bit) as [Is static]
     , cast(null as bit) as [Prohibit rebuild]
     , cast(null as varchar(128)) as [Signature]
 where 1 = 0

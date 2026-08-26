/*
Table ID: _.KeyDictionary

Description: >-
  Declared logical keys, the primary key and any alternate keys. Neither is
  built and neither is enforced; they say which column sets identify a row.
  A key is identified by its own columns, so it needs no name.

Lineage: >-
  Projected from validated Weaver document declarations by Weaver's own
  build, and maintained only by the catalogue DML a build appends. Never
  populated by a load.

Dependencies: []

Static: true

Prohibit rebuild: true

Has load procedure: false

Primary key: Item type, Item name, Schema name, Object name, Key type, Column set

Not null:
  - Signature

Schema:
  Item type: varchar(128)
  Item name: varchar(128)
  Schema name: varchar(128)
  Object name: varchar(128)
  Key type: varchar(128)
  Column set: varchar(1000)
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
  Key type: >-
    Primary key or Unique.
  Column set: >-
    The key's columns, comma-separated in declared order.
  Signature: >-
    Content hash of the object's source file, so a change can be detected.
*/
select cast(null as varchar(128)) as [Item type]
     , cast(null as varchar(128)) as [Item name]
     , cast(null as varchar(128)) as [Schema name]
     , cast(null as varchar(128)) as [Object name]
     , cast(null as varchar(128)) as [Key type]
     , cast(null as varchar(1000)) as [Column set]
     , cast(null as varchar(128)) as [Signature]
 where 1 = 0

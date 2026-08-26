/*
Table ID: _.Bookmark

Description: >-
  How far each loadable object has been loaded: the UTC instant immediately
  before its most recent clean load began. An incremental read asks for
  source changes after it, and a Static object is skipped once it holds
  anything other than the sentinel. Weaver's own build and load lifecycle
  maintain it; no declaration projects it and no load populates it.

Lineage: >-
  Maintained by Weaver's own build and load lifecycle: a build resets the
  objects it rebuilds and removes the rows of objects it no longer loads,
  and a clean load advances the object it loaded. Never authored, never
  projected from a declaration, and never populated by a load.

Dependencies: []

Static: true

Prohibit rebuild: true

Has load procedure: false

Primary key: Item type, Item name, Schema name, Object name

Not null:
  - Bookmark datetime

Schema:
  Item type: varchar(128)
  Item name: varchar(128)
  Schema name: varchar(128)
  Object name: varchar(128)
  Bookmark datetime: datetime2(6)

Column notes:
  Item type: >-
    Logical Weaver item type.
  Item name: >-
    Logical Weaver item name.
  Schema name: >-
    The object's schema.
  Object name: >-
    The object's name.
  Bookmark datetime: >-
    The UTC instant immediately before the most recent clean load began, or
    1900-01-01 00:00:00.000000 for an object that has not had one.
*/
select cast(null as varchar(128)) as [Item type]
     , cast(null as varchar(128)) as [Item name]
     , cast(null as varchar(128)) as [Schema name]
     , cast(null as varchar(128)) as [Object name]
     , cast(null as datetime2(6)) as [Bookmark datetime]
 where 1 = 0

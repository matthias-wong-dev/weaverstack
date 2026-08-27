/*
Table ID: _.LoadStatus

Description: >-
  How each loadable object's most recent load ended. One row per object per
  physical incarnation: a rebuild ends the incarnation and the row goes with
  it, so an absent row means no load has settled since the object was last
  built. Logical identity only, because where it is physically installed is
  the Installation's to say.

Lineage: >-
  Maintained by Weaver's own build and load lifecycle: a build removes the
  rows of objects it rebuilds or no longer loads, and each settled load
  records how it ended. Never authored, never projected from a declaration,
  and never populated by a load's own query.

Dependencies: []

Static: true

Prohibit rebuild: true

Has load procedure: false

Primary key: Item type, Item name, Schema name, Object name

Not null:
  - Result

Schema:
  Item type: varchar(128)
  Item name: varchar(128)
  Schema name: varchar(128)
  Object name: varchar(128)
  Workflow ID: varchar(128)
  Result: varchar(128)
  Started datetime: datetime2(6)
  Completed datetime: datetime2(6)
  Duration milliseconds: bigint

Column notes:
  Item type: >-
    Logical Weaver item type.
  Item name: >-
    Logical Weaver item name.
  Schema name: >-
    The object's schema.
  Object name: >-
    The object's name.
  Workflow ID: >-
    The workflow whose load produced this state, so the row can be read
    alongside the evidence in _.Log.
  Result: >-
    How the work ended.
  Started datetime: >-
    When the work started.
  Completed datetime: >-
    When the work settled.
  Duration milliseconds: >-
    How long the work took, in milliseconds.
*/
select cast(null as varchar(128)) as [Item type]
     , cast(null as varchar(128)) as [Item name]
     , cast(null as varchar(128)) as [Schema name]
     , cast(null as varchar(128)) as [Object name]
     , cast(null as varchar(128)) as [Workflow ID]
     , cast(null as varchar(128)) as [Result]
     , cast(null as datetime2(6)) as [Started datetime]
     , cast(null as datetime2(6)) as [Completed datetime]
     , cast(null as bigint) as [Duration milliseconds]
 where 1 = 0

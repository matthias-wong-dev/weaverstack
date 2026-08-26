/*
Table ID: _.TestStatus

Description: >-
  How each validation's most recent run ended. One row per validation per
  incarnation, as _.LoadStatus is for a loadable object: rebuilding the
  validation ends the incarnation and the row goes with it.

Lineage: >-
  Maintained by Weaver's own build and validation lifecycle: a build removes
  the rows of validations it rebuilds or no longer installs, and each
  settled validation records what it found. Never authored and never
  projected from a declaration.

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
  Test type: varchar(128)
  Workflow ID: varchar(128)
  Result: varchar(128)
  Started datetime: datetime2(6)
  Completed datetime: datetime2(6)
  Duration milliseconds: bigint
  Failure count: bigint

Column notes:
  Item type: >-
    Logical Weaver item type.
  Item name: >-
    Logical Weaver item name.
  Schema name: >-
    The object's schema.
  Object name: >-
    The object's name.
  Test type: >-
    Test or Assumption.
  Workflow ID: >-
    The workflow whose run produced this state, so the row can be read
    alongside the evidence in _.Log.
  Result: >-
    How the work ended.
  Started datetime: >-
    When the work started.
  Completed datetime: >-
    When the work settled.
  Duration milliseconds: >-
    How long the work took, in milliseconds.
  Failure count: >-
    How much disagreed: discrepancy rows for a Test, contradicting rows for
    an Assumption. Meaningful only for a validation that was evaluated.
*/
select cast(null as varchar(128)) as [Item type]
     , cast(null as varchar(128)) as [Item name]
     , cast(null as varchar(128)) as [Schema name]
     , cast(null as varchar(128)) as [Object name]
     , cast(null as varchar(128)) as [Test type]
     , cast(null as varchar(128)) as [Workflow ID]
     , cast(null as varchar(128)) as [Result]
     , cast(null as datetime2(6)) as [Started datetime]
     , cast(null as datetime2(6)) as [Completed datetime]
     , cast(null as bigint) as [Duration milliseconds]
     , cast(null as bigint) as [Failure count]
 where 1 = 0

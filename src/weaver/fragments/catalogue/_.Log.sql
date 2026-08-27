/*
Table ID: _.Log

Description: >-
  One row per settled unit of Weaver work. Operational evidence rather than
  installed state, so it is appended as work settles and no declaration is
  reconciled against it.

Lineage: >-
  Appended by Weaver's own runs as each unit of work settles. Never
  authored, never projected from a declaration, and never populated by a
  load.

Dependencies: []

Static: true

Prohibit rebuild: true

Has load procedure: false

Primary key: Log SK

Not null:
  - Workflow ID
  - Task type
  - Result

Schema:
  Log SK: varchar(128)
  Workflow ID: varchar(128)
  Task type: varchar(128)
  Target type: varchar(128)
  Target name: varchar(128)
  Schema name: varchar(128)
  Object name: varchar(128)
  Result: varchar(128)
  Started datetime: datetime2(6)
  Completed datetime: datetime2(6)
  Duration milliseconds: bigint
  Message: varchar(4000)
  Details: varchar(4000)

Column notes:
  Log SK: >-
    A meaningless immutable surrogate row key. Generated where the row is,
    because a Fabric Warehouse has no identity column and several sessions
    may append at once.
  Workflow ID: >-
    Correlates every row one workflow produced. A composed run shares one
    value across its operations.
  Task type: >-
    The kind of work that settled.
  Target type: >-
    The physical target's type.
  Target name: >-
    The physical target's name.
  Schema name: >-
    The object's schema.
  Object name: >-
    The object's name.
  Result: >-
    How the work ended. A load may also be Rejected, meaning it completed
    with rejected rows.
  Started datetime: >-
    When the work started.
  Completed datetime: >-
    When the work settled.
  Duration milliseconds: >-
    How long the work took, in milliseconds.
  Message: >-
    Concise human-readable information.
  Details: >-
    Structured task-specific detail, as JSON.
*/
select cast(null as varchar(128)) as [Log SK]
     , cast(null as varchar(128)) as [Workflow ID]
     , cast(null as varchar(128)) as [Task type]
     , cast(null as varchar(128)) as [Target type]
     , cast(null as varchar(128)) as [Target name]
     , cast(null as varchar(128)) as [Schema name]
     , cast(null as varchar(128)) as [Object name]
     , cast(null as varchar(128)) as [Result]
     , cast(null as datetime2(6)) as [Started datetime]
     , cast(null as datetime2(6)) as [Completed datetime]
     , cast(null as bigint) as [Duration milliseconds]
     , cast(null as varchar(4000)) as [Message]
     , cast(null as varchar(4000)) as [Details]
 where 1 = 0

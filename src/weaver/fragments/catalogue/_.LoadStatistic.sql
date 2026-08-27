/*
Table ID: _.LoadStatistic

Description: >-
  What each load did, as counts. Appended: a load's statistics are a fact
  about a moment, so a later rebuild does not remove them and the history of
  an object's loads accumulates.

Lineage: >-
  Appended by Weaver's own loads as each one settles. Never authored, never
  projected from a declaration, and never removed by a rebuild.

Dependencies: []

Static: true

Prohibit rebuild: true

Has load procedure: false

Primary key: Load statistic SK

Not null:
  - Workflow ID
  - Item type
  - Item name
  - Schema name
  - Object name

Schema:
  Load statistic SK: varchar(128)
  Workflow ID: varchar(128)
  Item type: varchar(128)
  Item name: varchar(128)
  Schema name: varchar(128)
  Object name: varchar(128)
  Started datetime: datetime2(6)
  Completed datetime: datetime2(6)
  Duration milliseconds: bigint
  Rows read: bigint
  Rows inserted: bigint
  Rows updated: bigint
  Rows deleted: bigint
  Rows rejected: bigint
  Is reload: bit
  Is static skip: bit

Column notes:
  Load statistic SK: >-
    A meaningless immutable surrogate row key. Generated where the row is,
    because a Fabric Warehouse has no identity column and several sessions
    may append at once.
  Workflow ID: >-
    Correlates every row one workflow produced.
  Item type: >-
    Logical Weaver item type.
  Item name: >-
    Logical Weaver item name.
  Schema name: >-
    The object's schema.
  Object name: >-
    The object's name.
  Started datetime: >-
    When the load started.
  Completed datetime: >-
    When the load settled.
  Duration milliseconds: >-
    How long the load took, in milliseconds.
  Rows read: >-
    What the source produced.
  Rows inserted: >-
    Rows the target gained.
  Rows updated: >-
    Rows the target changed.
  Rows deleted: >-
    Rows the target lost.
  Rows rejected: >-
    Incoming rows the load refused, kept in the reject table.
  Is reload: >-
    Whether the load re-read a window it had already read. False until
    reload is available.
  Is static skip: >-
    Whether a Static object was skipped because a clean load had already run
    for this incarnation.
*/
select cast(null as varchar(128)) as [Load statistic SK]
     , cast(null as varchar(128)) as [Workflow ID]
     , cast(null as varchar(128)) as [Item type]
     , cast(null as varchar(128)) as [Item name]
     , cast(null as varchar(128)) as [Schema name]
     , cast(null as varchar(128)) as [Object name]
     , cast(null as datetime2(6)) as [Started datetime]
     , cast(null as datetime2(6)) as [Completed datetime]
     , cast(null as bigint) as [Duration milliseconds]
     , cast(null as bigint) as [Rows read]
     , cast(null as bigint) as [Rows inserted]
     , cast(null as bigint) as [Rows updated]
     , cast(null as bigint) as [Rows deleted]
     , cast(null as bigint) as [Rows rejected]
     , cast(null as bit) as [Is reload]
     , cast(null as bit) as [Is static skip]
 where 1 = 0

/*
Table ID: _.Registry

Description: >-
  Objects Weaver currently certifies as installed. A physical table may
  exist without a row here, and Weaver then does not treat it as valid.
  Written last in a build, so its presence means everything the object
  needed succeeded.

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
  - Object type
  - Object role
  - Signature

Schema:
  Item type: varchar(128)
  Item name: varchar(128)
  Schema name: varchar(128)
  Object name: varchar(128)
  Object type: varchar(128)
  Object role: varchar(128)
  Signature: varchar(128)
  Build datetime: datetime2(6)

Column notes:
  Item type: >-
    Logical Weaver item type.
  Item name: >-
    Logical Weaver item name.
  Schema name: >-
    The object's schema.
  Object name: >-
    The object's name.
  Object type: >-
    What was installed: Folder, Table, View, File or Stored procedure.
  Object role: >-
    What the object is for: Data holds or shapes rows; Load does the work
    that fills one.
  Signature: >-
    Content hash of the object's source file, so a change can be detected.
  Build datetime: >-
    When this row was published, shared by every row one completed build
    wrote.
*/
select cast(null as varchar(128)) as [Item type]
     , cast(null as varchar(128)) as [Item name]
     , cast(null as varchar(128)) as [Schema name]
     , cast(null as varchar(128)) as [Object name]
     , cast(null as varchar(128)) as [Object type]
     , cast(null as varchar(128)) as [Object role]
     , cast(null as varchar(128)) as [Signature]
     , cast(null as datetime2(6)) as [Build datetime]
 where 1 = 0

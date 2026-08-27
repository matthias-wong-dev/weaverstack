/*
Table ID: _.ColumnDictionary

Description: >-
  What an author said about a column, plus Weaver's own surrogate. Purely
  descriptive: it holds the columns that carry a note, not every column of
  every object. Ordinals, types and nullability are physical and are
  recorded separately, so nothing here depends on reading a built table.

Lineage: >-
  Projected from validated Weaver document declarations by Weaver's own
  build, and maintained only by the catalogue DML a build appends. Never
  populated by a load.

Dependencies: []

Static: true

Prohibit rebuild: true

Has load procedure: false

Primary key: Item type, Item name, Schema name, Object name, Column name

Not null:
  - Signature

Schema:
  Item type: varchar(128)
  Item name: varchar(128)
  Schema name: varchar(128)
  Object name: varchar(128)
  Column name: varchar(128)
  Description: varchar(4000)
  Description reference: varchar(128)
  Is identity: bit
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
  Column name: >-
    The column.
  Description: >-
    What this column is.
  Description reference: >-
    The $$Schema.Object the description was copied from, when it was
    declared as a reference rather than written here.
  Is identity: >-
    Whether this is Weaver's managed surrogate column.
  Signature: >-
    Content hash of the object's source file, so a change can be detected.
*/
select cast(null as varchar(128)) as [Item type]
     , cast(null as varchar(128)) as [Item name]
     , cast(null as varchar(128)) as [Schema name]
     , cast(null as varchar(128)) as [Object name]
     , cast(null as varchar(128)) as [Column name]
     , cast(null as varchar(4000)) as [Description]
     , cast(null as varchar(128)) as [Description reference]
     , cast(null as bit) as [Is identity]
     , cast(null as varchar(128)) as [Signature]
 where 1 = 0

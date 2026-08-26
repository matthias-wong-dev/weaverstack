/*
Table ID: _.SchemaDictionary

Description: >-
  The declared schemas an installation uses, and what they are for.

Lineage: >-
  Projected from validated Weaver document declarations by Weaver's own
  build, and maintained only by the catalogue DML a build appends. Never
  populated by a load.

Dependencies: []

Static: true

Prohibit rebuild: true

Has load procedure: false

Primary key: Item type, Item name, Schema name

Not null:
  - Signature

Schema:
  Item type: varchar(128)
  Item name: varchar(128)
  Schema name: varchar(128)
  Description: varchar(4000)
  Description reference: varchar(128)
  Signature: varchar(128)

Column notes:
  Item type: >-
    Logical Weaver item type.
  Item name: >-
    Logical Weaver item name.
  Schema name: >-
    The schema.
  Description: >-
    What this schema is.
  Description reference: >-
    The $$Schema.Object the description was copied from, when it was
    declared as a reference rather than written here.
  Signature: >-
    Content hash of the schema declaration, so a change can be detected.
*/
select cast(null as varchar(128)) as [Item type]
     , cast(null as varchar(128)) as [Item name]
     , cast(null as varchar(128)) as [Schema name]
     , cast(null as varchar(4000)) as [Description]
     , cast(null as varchar(128)) as [Description reference]
     , cast(null as varchar(128)) as [Signature]
 where 1 = 0

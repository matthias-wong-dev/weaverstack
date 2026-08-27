/*
Table ID: _.TestDictionary

Description: >-
  Tests and Assumptions, the estate's declared validation. It describes the
  logical authored declaration, not the procedure or module the validation
  compiles to: that is a physical artefact and Registry certifies it. One
  table for both kinds because the same questions are questions of each, and
  because Tests and Assumptions share one logical namespace within an item
  and so cannot both claim a key.

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
  - Test type
  - Signature

Schema:
  Item type: varchar(128)
  Item name: varchar(128)
  Schema name: varchar(128)
  Object name: varchar(128)
  Test type: varchar(128)
  Description: varchar(4000)
  Description reference: varchar(128)
  Primary key: varchar(1000)
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
  Test type: >-
    Test compares an expected relation with an actual one; Assumption
    returns the rows that contradict it.
  Description: >-
    What this validation is.
  Description reference: >-
    The $$Schema.Object the description was copied from, when it was
    declared as a reference rather than written here.
  Primary key: >-
    A Test's declared key, comma-separated in declared order. It correlates
    the two sides of the comparison and does not change what is counted.
    Null for a Test that declares none, and always null for an Assumption,
    which has one side to correlate.
  Signature: >-
    Content hash of the validation's source file, so a change can be
    detected.
*/
select cast(null as varchar(128)) as [Item type]
     , cast(null as varchar(128)) as [Item name]
     , cast(null as varchar(128)) as [Schema name]
     , cast(null as varchar(128)) as [Object name]
     , cast(null as varchar(128)) as [Test type]
     , cast(null as varchar(4000)) as [Description]
     , cast(null as varchar(128)) as [Description reference]
     , cast(null as varchar(1000)) as [Primary key]
     , cast(null as varchar(128)) as [Signature]
 where 1 = 0

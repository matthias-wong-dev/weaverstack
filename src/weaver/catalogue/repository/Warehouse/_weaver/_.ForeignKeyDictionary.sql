/*
Table ID: _.ForeignKeyDictionary

Description: >-
  Declared relationships to primary objects, an ER model rather than
  database constraints. Nothing is enforced. Because a relationship has no
  name, the row is the edge: every column is part of the key, so two objects
  may be related several times over and an object may reference itself. The
  owning item scopes the foreign side; the primary side carries item
  identity because it may cross items.

Lineage: >-
  Projected from validated Weaver document declarations by Weaver's own
  build, and maintained only by the catalogue DML a build appends. Never
  populated by a load.

Dependencies: []

Static: true

Prohibit rebuild: true

Has load procedure: false

Primary key: Item type, Item name, Foreign schema name, Foreign object name, Foreign column set, Primary item type, Primary item name, Primary schema name, Primary object name, Primary column set

Not null:
  - Signature

Schema:
  Item type: varchar(128)
  Item name: varchar(128)
  Foreign schema name: varchar(128)
  Foreign object name: varchar(128)
  Foreign column set: varchar(1000)
  Primary item type: varchar(128)
  Primary item name: varchar(128)
  Primary schema name: varchar(128)
  Primary object name: varchar(128)
  Primary column set: varchar(1000)
  Signature: varchar(128)

Column notes:
  Item type: >-
    Logical Weaver item type.
  Item name: >-
    Logical Weaver item name.
  Foreign schema name: >-
    The schema of the object declaring the relationship.
  Foreign object name: >-
    The name of the object declaring the relationship.
  Foreign column set: >-
    The foreign columns, comma-separated in declared order.
  Primary item type: >-
    The primary item's type.
  Primary item name: >-
    The primary item's name.
  Primary schema name: >-
    The primary schema.
  Primary object name: >-
    The primary object.
  Primary column set: >-
    The primary columns, paired in order with the foreign ones.
  Signature: >-
    Content hash of the object's source file, so a change can be detected.
*/
select cast(null as varchar(128)) as [Item type]
     , cast(null as varchar(128)) as [Item name]
     , cast(null as varchar(128)) as [Foreign schema name]
     , cast(null as varchar(128)) as [Foreign object name]
     , cast(null as varchar(1000)) as [Foreign column set]
     , cast(null as varchar(128)) as [Primary item type]
     , cast(null as varchar(128)) as [Primary item name]
     , cast(null as varchar(128)) as [Primary schema name]
     , cast(null as varchar(128)) as [Primary object name]
     , cast(null as varchar(1000)) as [Primary column set]
     , cast(null as varchar(128)) as [Signature]
 where 1 = 0

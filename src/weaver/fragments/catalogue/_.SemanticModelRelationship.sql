/*
Table ID: _.SemanticModelRelationship

Description: Native semantic model metadata.

Lineage: Projected from deployed semantic definitions during Build.

Dependencies: []

Static: true

Prohibit rebuild: true

Has load procedure: false

Primary key: Item type, Item name, Schema name, Object name, Relationship name

Not null:
  - Signature

Schema:
  Item type: varchar(128)
  Item name: varchar(128)
  Schema name: varchar(128)
  Object name: varchar(128)
  Relationship name: varchar(128)
  From table: varchar(128)
  From column: varchar(128)
  To table: varchar(128)
  To column: varchar(128)
  From cardinality: varchar(128)
  To cardinality: varchar(128)
  Cross filtering behavior: varchar(128)
  Is active: bit
  Signature: varchar(128)
*/
select cast(null as varchar(128)) as [Item type]
     , cast(null as varchar(128)) as [Item name]
     , cast(null as varchar(128)) as [Schema name]
     , cast(null as varchar(128)) as [Object name]
     , cast(null as varchar(128)) as [Relationship name]
     , cast(null as varchar(128)) as [From table]
     , cast(null as varchar(128)) as [From column]
     , cast(null as varchar(128)) as [To table]
     , cast(null as varchar(128)) as [To column]
     , cast(null as varchar(128)) as [From cardinality]
     , cast(null as varchar(128)) as [To cardinality]
     , cast(null as varchar(128)) as [Cross filtering behavior]
     , cast(null as bit) as [Is active]
     , cast(null as varchar(128)) as [Signature]
 where 1 = 0

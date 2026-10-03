/*
Table ID: _.SemanticModelTable

Description: Native semantic model metadata.

Lineage: Projected from deployed semantic definitions during Build.

Dependencies: []

Static: true

Prohibit rebuild: true

Has load procedure: false

Primary key: Item type, Item name, Schema name, Object name, Table name

Not null:
  - Signature

Schema:
  Item type: varchar(128)
  Item name: varchar(128)
  Schema name: varchar(128)
  Object name: varchar(128)
  Table name: varchar(128)
  Description: varchar(max)
  Source binding: varchar(max)
  Properties: varchar(max)
  Provenance: varchar(max)
  Signature: varchar(128)
*/
select cast(null as varchar(128)) as [Item type]
     , cast(null as varchar(128)) as [Item name]
     , cast(null as varchar(128)) as [Schema name]
     , cast(null as varchar(128)) as [Object name]
     , cast(null as varchar(128)) as [Table name]
     , cast(null as varchar(max)) as [Description]
     , cast(null as varchar(max)) as [Source binding]
     , cast(null as varchar(max)) as [Properties]
     , cast(null as varchar(max)) as [Provenance]
     , cast(null as varchar(128)) as [Signature]
 where 1 = 0

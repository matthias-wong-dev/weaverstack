/*
Table ID: _.SemanticModel

Description: Native semantic model metadata.

Lineage: Projected from deployed semantic definitions during Build.

Dependencies: []

Static: true

Prohibit rebuild: true

Has load procedure: false

Primary key: Item type, Item name

Not null:
  - Signature

Schema:
  Item type: varchar(128)
  Item name: varchar(128)
  Description: varchar(max)
  Signature: varchar(128)
*/
select cast(null as varchar(128)) as [Item type]
     , cast(null as varchar(128)) as [Item name]
     , cast(null as varchar(max)) as [Description]
     , cast(null as varchar(128)) as [Signature]
 where 1 = 0

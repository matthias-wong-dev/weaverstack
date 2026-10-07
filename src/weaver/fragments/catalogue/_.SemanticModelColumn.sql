/*
Table ID: _.SemanticModelColumn

Description: Native semantic model metadata.

Lineage: Projected from deployed semantic definitions during Build.

Dependencies: []

Static: true

Prohibit rebuild: true

Has load procedure: false

Primary key: Item type, Item name, Schema name, Object name, Table name, Column name

Not null:
  - Signature

Schema:
  Item type: varchar(128)
  Item name: varchar(128)
  Schema name: varchar(128)
  Object name: varchar(128)
  Table name: varchar(128)
  Column name: varchar(128)
  Description: varchar(max)
  Data type: varchar(128)
  Column type: varchar(128)
  Source column: varchar(128)
  Expression: varchar(max)
  Signature: varchar(128)
*/
select cast(null as varchar(128)) as [Item type]
     , cast(null as varchar(128)) as [Item name]
     , cast(null as varchar(128)) as [Schema name]
     , cast(null as varchar(128)) as [Object name]
     , cast(null as varchar(128)) as [Table name]
     , cast(null as varchar(128)) as [Column name]
     , cast(null as varchar(max)) as [Description]
     , cast(null as varchar(128)) as [Data type]
     , cast(null as varchar(128)) as [Column type]
     , cast(null as varchar(128)) as [Source column]
     , cast(null as varchar(max)) as [Expression]
     , cast(null as varchar(128)) as [Signature]
 where 1 = 0

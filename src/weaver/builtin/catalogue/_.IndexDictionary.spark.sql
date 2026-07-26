/*
Table ID: _.IndexDictionary

Description: >-
  Declared logical keys — the primary key and any alternate keys. Neither is
  built and neither is enforced; they say which column sets identify a row.
  A key is identified by its own columns, so it needs no name.

Lineage: >-
  Projected from validated SES declarations by Weaver's own build, and
  maintained only by the catalogue DML a build appends. Never populated by a
  load.

Dependencies: []

Static: true

Prohibit rebuild: true

Primary key: repository, target_type, schema_name, object_name, index_type, column_set

Not null:
  - signature

Schema:
  repository: string
  target_type: string
  schema_name: string
  object_name: string
  index_type: string
  column_set: string
  signature: string

Column notes:
  repository: >-
    The SES repository this row belongs to.
  target_type: >-
    The physical installation type, lakehouse or warehouse. A repository has
    at most one current installation of each.
  schema_name: >-
    The object's schema.
  object_name: >-
    The object's name.
  index_type: >-
    primary_key or unique.
  column_set: >-
    The key's columns, comma-separated in declared order.
  signature: >-
    Content hash of the object's source file, so a change can be detected.
*/
select cast(null as string) as `repository`
     , cast(null as string) as `target_type`
     , cast(null as string) as `schema_name`
     , cast(null as string) as `object_name`
     , cast(null as string) as `index_type`
     , cast(null as string) as `column_set`
     , cast(null as string) as `signature`
 where 1 = 0

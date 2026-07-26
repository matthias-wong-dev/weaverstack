/*
Table ID: _.ForeignKeyDictionary

Description: >-
  Declared relationships to parent objects — an ER model rather than
  database constraints. Nothing is enforced. Because a relationship has no
  name, the row is the edge: every column is part of the key, so two objects
  may be related several times over and an object may reference itself. The
  reference stays logical and inherits the owner's target type; a Warehouse
  object's parent is a Warehouse object.

Lineage: >-
  Projected from validated SES declarations by Weaver's own build, and
  maintained only by the catalogue DML a build appends. Never populated by a
  load.

Dependencies: []

Static: true

Prohibit rebuild: true

Primary key: repository, target_type, schema_name, object_name, column_set, reference_repository, reference_schema_name, reference_object_name, reference_column_set

Not null:
  - signature

Schema:
  repository: string
  target_type: string
  schema_name: string
  object_name: string
  column_set: string
  reference_repository: string
  reference_schema_name: string
  reference_object_name: string
  reference_column_set: string
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
  column_set: >-
    This object's columns, comma-separated in declared order.
  reference_repository: >-
    The parent's repository. Today always the owner's own.
  reference_schema_name: >-
    The parent's schema.
  reference_object_name: >-
    The parent's name.
  reference_column_set: >-
    The parent's columns, paired in order with this object's.
  signature: >-
    Content hash of the object's source file, so a change can be detected.
*/
select cast(null as string) as `repository`
     , cast(null as string) as `target_type`
     , cast(null as string) as `schema_name`
     , cast(null as string) as `object_name`
     , cast(null as string) as `column_set`
     , cast(null as string) as `reference_repository`
     , cast(null as string) as `reference_schema_name`
     , cast(null as string) as `reference_object_name`
     , cast(null as string) as `reference_column_set`
     , cast(null as string) as `signature`
 where 1 = 0

/*
Table ID: _.TableDictionary

Description: >-
  Tables and views together — they are described the same way and a reader
  asks the same questions of both. Everything here is declared in SES;
  nothing is read back from the physical object.

Lineage: >-
  Projected from validated SES declarations by Weaver's own build, and
  maintained only by the catalogue DML a build appends. Never populated by a
  load.

Dependencies: []

Static: true

Prohibit rebuild: true

Primary key: repository, target_type, schema_name, object_name

Not null:
  - object_type
  - signature

Schema:
  repository: string
  target_type: string
  schema_name: string
  object_name: string
  object_type: string
  description: string
  description_reference: string
  lineage: string
  lineage_reference: string
  primary_key: string
  not_null_columns: string
  identity_column: string
  comparison_columns: string
  is_incremental: boolean
  is_static: boolean
  prohibit_rebuild: boolean
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
  object_type: >-
    table or view.
  description: >-
    What this object is.
  description_reference: >-
    The $$Schema.Object the description was copied from, when it was
    declared as a reference rather than written here.
  lineage: >-
    Where this object's data comes from.
  lineage_reference: >-
    The $$Schema.Object the lineage was copied from, if any.
  primary_key: >-
    The primary key's columns, in declared order.
  not_null_columns: >-
    Columns declared not null, beyond the primary key.
  identity_column: >-
    Weaver's managed surrogate column, when one is declared.
  comparison_columns: >-
    Columns whose change drives an upsert.
  is_incremental: >-
    Whether load accumulates rows rather than replacing them.
  is_static: >-
    Whether the object is loaded once rather than refreshed.
  prohibit_rebuild: >-
    Whether build may drop and recreate this object.
  signature: >-
    Content hash of the object's source file, so a change can be detected.
*/
select cast(null as string) as `repository`
     , cast(null as string) as `target_type`
     , cast(null as string) as `schema_name`
     , cast(null as string) as `object_name`
     , cast(null as string) as `object_type`
     , cast(null as string) as `description`
     , cast(null as string) as `description_reference`
     , cast(null as string) as `lineage`
     , cast(null as string) as `lineage_reference`
     , cast(null as string) as `primary_key`
     , cast(null as string) as `not_null_columns`
     , cast(null as string) as `identity_column`
     , cast(null as string) as `comparison_columns`
     , cast(null as boolean) as `is_incremental`
     , cast(null as boolean) as `is_static`
     , cast(null as boolean) as `prohibit_rebuild`
     , cast(null as string) as `signature`
 where 1 = 0

/*
Table ID: _.ColumnDictionary

Description: >-
  What an author said about a column, plus Weaver's own surrogate. Purely
  descriptive: it holds the columns that carry a note, not every column of
  every object. Ordinals, types and nullability are physical and are
  recorded separately, so nothing here depends on reading a built table.

Lineage: >-
  Projected from validated SES declarations by Weaver's own build, and
  maintained only by the catalogue DML a build appends. Never populated by a
  load.

Dependencies: []

Static: true

Prohibit rebuild: true

Primary key: repository, target_type, schema_name, object_name, column_name

Not null:
  - signature

Schema:
  repository: string
  target_type: string
  schema_name: string
  object_name: string
  column_name: string
  description: string
  description_reference: string
  is_identity: boolean
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
  column_name: >-
    The column.
  description: >-
    What this column is.
  description_reference: >-
    The $$Schema.Object the description was copied from, when it was
    declared as a reference rather than written here.
  is_identity: >-
    Whether this is Weaver's managed surrogate column.
  signature: >-
    Content hash of the object's source file, so a change can be detected.
*/
select cast(null as string) as `repository`
     , cast(null as string) as `target_type`
     , cast(null as string) as `schema_name`
     , cast(null as string) as `object_name`
     , cast(null as string) as `column_name`
     , cast(null as string) as `description`
     , cast(null as string) as `description_reference`
     , cast(null as boolean) as `is_identity`
     , cast(null as string) as `signature`
 where 1 = 0

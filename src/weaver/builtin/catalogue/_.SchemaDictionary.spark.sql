/*
Table ID: _.SchemaDictionary

Description: >-
  The declared schemas an installation uses, and what they are for.

Lineage: >-
  Projected from validated SES declarations by Weaver's own build, and
  maintained only by the catalogue DML a build appends. Never populated by a
  load.

Dependencies: []

Static: true

Prohibit rebuild: true

Primary key: repository, target_type, schema_name

Not null:
  - signature

Schema:
  repository: string
  target_type: string
  schema_name: string
  description: string
  description_reference: string
  signature: string

Column notes:
  repository: >-
    The SES repository this row belongs to.
  target_type: >-
    The physical installation type, lakehouse or warehouse. A repository has
    at most one current installation of each.
  schema_name: >-
    The schema.
  description: >-
    What this schema is.
  description_reference: >-
    The $$Schema.Object the description was copied from, when it was
    declared as a reference rather than written here.
  signature: >-
    Content hash of the schema declaration, so a change can be detected.
*/
select cast(null as string) as `repository`
     , cast(null as string) as `target_type`
     , cast(null as string) as `schema_name`
     , cast(null as string) as `description`
     , cast(null as string) as `description_reference`
     , cast(null as string) as `signature`
 where 1 = 0

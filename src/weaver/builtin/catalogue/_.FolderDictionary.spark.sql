/*
Table ID: _.FolderDictionary

Description: >-
  Managed folders. A folder keeps its two-part SES identity rather than
  being reduced to a path, and its file key is the scope of what Weaver
  manages inside it — reconciliation deletes nothing outside that.

Lineage: >-
  Projected from validated SES declarations by Weaver's own build, and
  maintained only by the catalogue DML a build appends. Never populated by a
  load.

Dependencies: []

Static: true

Prohibit rebuild: true

Primary key: repository, target_type, schema_name, object_name

Not null:
  - signature

Schema:
  repository: string
  target_type: string
  schema_name: string
  object_name: string
  description: string
  description_reference: string
  lineage: string
  lineage_reference: string
  file_key: string
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
  description: >-
    What this folder is.
  description_reference: >-
    The $$Schema.Object the description was copied from, when it was
    declared as a reference rather than written here.
  lineage: >-
    Where this object's data comes from.
  lineage_reference: >-
    The $$Schema.Object the lineage was copied from, if any.
  file_key: >-
    The glob patterns Weaver manages, in declared order.
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
     , cast(null as string) as `description`
     , cast(null as string) as `description_reference`
     , cast(null as string) as `lineage`
     , cast(null as string) as `lineage_reference`
     , cast(null as string) as `file_key`
     , cast(null as boolean) as `is_incremental`
     , cast(null as boolean) as `is_static`
     , cast(null as boolean) as `prohibit_rebuild`
     , cast(null as string) as `signature`
 where 1 = 0

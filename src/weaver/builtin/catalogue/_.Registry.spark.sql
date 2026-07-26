/*
Table ID: _.Registry

Description: >-
  Objects Weaver currently certifies as installed. A physical table may
  exist without a row here, and Weaver then does not treat it as valid.
  Written last in a build, so its presence means everything the object
  needed succeeded.

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
  - object_role
  - signature

Schema:
  repository: string
  target_type: string
  schema_name: string
  object_name: string
  object_type: string
  object_role: string
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
    What was installed: folder, table or view.
  object_role: >-
    What the object is for: data holds or shapes rows; load does work, which
    arrives with stored procedures.
  signature: >-
    Content hash of the object's source file, so a change can be detected.
*/
select cast(null as string) as `repository`
     , cast(null as string) as `target_type`
     , cast(null as string) as `schema_name`
     , cast(null as string) as `object_name`
     , cast(null as string) as `object_type`
     , cast(null as string) as `object_role`
     , cast(null as string) as `signature`
 where 1 = 0

/*
Table ID: _.Alias

Description: >-
  Cross-engine publication: the name a native object presents in another
  target type. This is where the estate's graph crosses engines, so it is
  kept apart from Dependency — composing Dependency, Alias and Registry is
  what yields the whole DAG, and only that composition may cross. The alias
  target type is part of the identity because there will be more kinds of
  alias than two.

Lineage: >-
  Projected from validated SES declarations by Weaver's own build, and
  maintained only by the catalogue DML a build appends. Never populated by a
  load.

Dependencies: []

Static: true

Prohibit rebuild: true

Primary key: repository, target_type, schema_name, object_name, alias_target_type

Not null:
  - alias_schema_name
  - alias_object_name
  - signature

Schema:
  repository: string
  target_type: string
  schema_name: string
  object_name: string
  alias_target_type: string
  alias_schema_name: string
  alias_object_name: string
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
  alias_target_type: >-
    The target type the alias is published into.
  alias_schema_name: >-
    The schema the alias is published under.
  alias_object_name: >-
    The name the alias is published under.
  signature: >-
    Content hash of the publishing object's source file, so a change can be
    detected.
*/
select cast(null as string) as `repository`
     , cast(null as string) as `target_type`
     , cast(null as string) as `schema_name`
     , cast(null as string) as `object_name`
     , cast(null as string) as `alias_target_type`
     , cast(null as string) as `alias_schema_name`
     , cast(null as string) as `alias_object_name`
     , cast(null as string) as `signature`
 where 1 = 0

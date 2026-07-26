/*
Table ID: _.Dependency

Description: >-
  One row per resolved dependency edge. The owning object is installation-
  scoped, but the reference is a three-part logical name and inherits the
  owner's target type — a Warehouse object resolves its dependencies in the
  Warehouse. Crossing engines is an alias, recorded separately, not a
  dependency that quietly changes namespace.

Lineage: >-
  Projected from validated SES declarations by Weaver's own build, and
  maintained only by the catalogue DML a build appends. Never populated by a
  load.

Dependencies: []

Static: true

Prohibit rebuild: true

Primary key: repository, target_type, schema_name, object_name, dependency_repository, dependency_schema_name, dependency_object_name

Not null:
  - signature

Schema:
  repository: string
  target_type: string
  schema_name: string
  object_name: string
  dependency_repository: string
  dependency_schema_name: string
  dependency_object_name: string
  is_within_repository: boolean
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
  dependency_repository: >-
    The dependency's repository, or the physical item a three-part external
    reference named.
  dependency_schema_name: >-
    The dependency's schema.
  dependency_object_name: >-
    The dependency's name.
  is_within_repository: >-
    False when the author named a physical target in three parts, which is
    allowed and is not a managed object of this repository.
  signature: >-
    Content hash of the owning object's source file, so a change can be
    detected.
*/
select cast(null as string) as `repository`
     , cast(null as string) as `target_type`
     , cast(null as string) as `schema_name`
     , cast(null as string) as `object_name`
     , cast(null as string) as `dependency_repository`
     , cast(null as string) as `dependency_schema_name`
     , cast(null as string) as `dependency_object_name`
     , cast(null as boolean) as `is_within_repository`
     , cast(null as string) as `signature`
 where 1 = 0

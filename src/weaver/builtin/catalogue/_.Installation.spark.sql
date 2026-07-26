/*
Table ID: _.Installation

Description: >-
  One row per repository installation — a repository against one physical
  target type. The bound item's name is an attribute, never identity:
  rebinding to a different Lakehouse updates this row rather than adding a
  second installation.

Lineage: >-
  Projected from validated SES declarations by Weaver's own build, and
  maintained only by the catalogue DML a build appends. Never populated by a
  load.

Dependencies: []

Static: true

Prohibit rebuild: true

Primary key: repository, target_type

Not null:
  - target_name
  - weaver_version
  - signature

Schema:
  repository: string
  target_type: string
  target_name: string
  weaver_version: string
  signature: string

Column notes:
  repository: >-
    The SES repository this row belongs to.
  target_type: >-
    The physical installation type, lakehouse or warehouse. A repository has
    at most one current installation of each.
  target_name: >-
    The physical item currently bound to this installation.
  weaver_version: >-
    The Weaver version that last reconciled this installation.
  signature: >-
    Content hash of the repository as a whole, so a change can be detected.
*/
select cast(null as string) as `repository`
     , cast(null as string) as `target_type`
     , cast(null as string) as `target_name`
     , cast(null as string) as `weaver_version`
     , cast(null as string) as `signature`
 where 1 = 0

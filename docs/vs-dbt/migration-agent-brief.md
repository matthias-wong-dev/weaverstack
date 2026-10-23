# Migrate a dbt project to Weaver

You are migrating an existing dbt project on Microsoft Fabric to Weaver.

Weaver is a declarative framework for data engineering on Microsoft Fabric. A Weaver project declares Tables, Views, Folders, Shortcuts, Tests and Assumptions using ordinary SQL, Python and metadata. Weaver manages deployment, dependencies, incremental loading, testing and operational state.

Express the dbt project’s intent using Weaver’s native architecture rather than mechanically reproducing dbt’s implementation. Inspect the source, confirm the architecture decisions with the user, then implement and validate the migration.

## Migration contract

Produce a working Weaver project that preserves the original business logic, data outputs, incremental behaviour and validations.

Every in-scope dbt resource must have a corresponding Weaver implementation, documented alternative or explicitly reported gap.

Preserve the original dbt project. Do not perform destructive operations or deploy to production without authorisation.

## 1. Understand the source project

Run:

```console
dbt parse
```

Treat `target/manifest.json` as the authoritative inventory of models, dependencies, sources, tests and other resources.

Use `dbt compile` where necessary to inspect generated SQL, subject to the permissions and execution requirements of the source project.

Reference: [dbt parse](https://docs.getdbt.com/reference/commands/parse).

## 2. Confirm migration decisions with the user

Before generating Weaver definitions, inspect the source project and propose answers to the following questions.

Ask the user to confirm or modify your recommendations. Group the questions into one concise interaction. Do not ask questions whose answers are already clear from the source project.

### A. Source environment and scope

Which dbt target/profile and project scope should be migrated? All enabled models, or a selected subset? Should seeds, snapshots, tests and custom hooks be included?

**Default:** Migrate all enabled resources and report unsupported features.

### B. Fabric destinations

Which Fabric workspace, Lakehouses and Warehouses should the new Weaver project use?

Propose logical Weaver item names, physical Fabric item names, a catalogue Warehouse and, where required, a Fabric Environment.

Should the project use new destinations or existing items?

**Default:** Use new, isolated development destinations. Preserve the original SQL engine used by each model.

### C. Names and schemas

Should schema, table, view and column names remain unchanged, or should they be standardised during migration?

**Default:** Preserve existing business-facing names.

### D. External references

Should references to other Fabric items use Weaver Shortcuts, or retain direct physical references where supported?

**Default:** Use logical Shortcuts for Weaver-managed producers and physical Shortcuts for external Fabric data wherever supported. Use direct references only where appropriate.

Account for differences in supported Shortcut types and workspace boundaries.

### E. Incremental loading

Should incremental models retain their current change-selection strategy, or be adapted to use Weaver’s per-consumer bookmarks?

Inspect the source’s available timestamps, CDC, change history, deletion handling and late-arriving records before recommending a strategy.

**Default:** Preserve existing incremental semantics. Recommend bookmark-driven selection where change evidence is sufficient and the resulting behaviour can be validated.

Do not silently substitute bookmarks for target-derived watermarks or other existing state mechanisms.

### F. Existing data and historical state

Should migrated tables start empty and be rebuilt, or should existing target data, snapshots and history be preserved?

Identify any objects that cannot safely be reconstructed.

**Default:** Build new targets without modifying existing data. Explicitly plan historical-state preservation where required.

### G. Validation and deployment

Is an isolated Fabric workspace available for Build, Load and Test? Can the agent read the original datasets to compare results?

Should the migration stop at validated project files, or proceed to a fully tested non-production deployment?

**Default:** Perform local validation without deployment unless explicitly authorised.

For unusual resources such as custom dbt materialisations, macros, packages and snapshots, ask additional targeted questions only when necessary.

## 3. Implement the Weaver project

Follow the architecture and authoring contracts in the documentation.

### Architecture and configuration

- [Design philosophy](https://docs.weaverstack.dev/core-concepts/design-philosophy/)
- [Projects and estates](https://docs.weaverstack.dev/core-concepts/projects-and-estates/)
- [Weaver documents](https://docs.weaverstack.dev/reference/weaver-documents/overview/)
- [Workspace configuration](https://docs.weaverstack.dev/reference/configuration-files/workspace-config/)

### Data engineering

- [Tables](https://docs.weaverstack.dev/reference/weaver-documents/table/)
- [Views](https://docs.weaverstack.dev/reference/weaver-documents/view/)
- [Shortcuts](https://docs.weaverstack.dev/reference/weaver-documents/shortcut/)
- [Incremental data processing and bookmarks](https://docs.weaverstack.dev/advanced/incremental-data-processing/)
- [Protecting existing data](https://docs.weaverstack.dev/advanced/protecting-data/)

### Validation

- [Tests](https://docs.weaverstack.dev/reference/weaver-documents/test/)
- [Assumptions](https://docs.weaverstack.dev/reference/weaver-documents/assumption/)
- [Build, Load and Test](https://docs.weaverstack.dev/core-concepts/build-load-and-test/)

Use these specifications to implement the project. Prefer native Weaver capabilities rather than recreating dbt-specific runtime machinery.

## 4. Validate

Run this throughout the migration and once after completing it:

```console
weaver check
```

Reconcile the migrated project against `target/manifest.json`. Account for every in-scope resource.

Where authorised, perform a Build dry-run:

```console
weaver build --dry-run
```

Where authorised and an isolated Fabric environment is available, deploy there and compare schemas, row counts, keys, calculated results, incremental updates, deletes and test outcomes.

A successful `dbt parse` and `weaver check` establish structural validity. They do not establish functional data equivalence.

## 5. Deliver

Provide the complete Weaver project, a dbt-to-Weaver resource mapping, the configuration decisions made, validation evidence and unresolved migration gaps.

The migration is complete only when the required behaviour has been preserved and independently verified.

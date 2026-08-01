# Weaver Backlog

## Core build and execution

1. **Implement basic ETL for a single Weaver item**
   - The lifecycle is in place: load artefacts are claimed, registered, signed,
     built, pruned and deployed to `Files/_/Load`, with a generated procedure per
     Warehouse table. What remains is the generated bodies themselves, and
     running them.
   - `SPARK_ETL_TEMPLATE_VERSION` and `TSQL_ETL_TEMPLATE_VERSION` exist so
     replacing a proxy body rebuilds exactly what it renders.
2. **Implement ETL across Weaver items using catalogue dependencies**
3. **Add ETL unit tests and document the execution assumptions**
4. **Implement bookmarks for incremental loading**

## Platform support

5. **Support semantic models**
6. **Support Fabric SQL Database**
   - Reuse as much of the Warehouse T-SQL implementation as possible.
   - Isolate the remaining target-specific behaviour.

## Performance and distribution

7. **Optimise repository push to OneLake**
8. **Optimise build planning and execution times**
9. **Publish WeaverStack to PyPI**
    - Support ordinary `pip install weaverstack`.
    - Ensure installation works cleanly within Microsoft Fabric environments.

## Developer and user experience

10. **Build a Weaver GUI over the catalogue**
    - Run as a Fabric custom web application.
    - Provide workspace, item, document, dependency, build-status and catalogue views.

11. **Generate HTML documentation into `docs/`**
    - Publish through GitHub Pages.
    - Include repository structure, Weaver documents, bindings, aliases, build behaviour, ETL and catalogue concepts.

12. **Create built-in examples**
    - A minimal single-item example.
    - A realistic multi-item example covering aliases, dependencies, ETL, bookmarks and multiple Fabric target types.

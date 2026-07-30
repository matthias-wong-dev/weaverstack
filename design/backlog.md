# Weaver Backlog

## Core build and execution

1. **Implement basic ETL for a single Weaver item**
2. **Implement cross-Lakehouse and cross-Warehouse builds through aliases**
3. **Implement ETL across Weaver items using catalogue dependencies**
4. **Add ETL unit tests and document the execution assumptions**
5. **Implement bookmarks for incremental loading**

## Platform support

6. **Support semantic models**
7. **Support Fabric SQL Database**
   - Reuse as much of the Warehouse T-SQL implementation as possible.
   - Isolate the remaining target-specific behaviour.

## Performance and distribution

8. **Optimise repository push to OneLake**
9. **Optimise build planning and execution times**
10. **Publish WeaverStack to PyPI**
    - Support ordinary `pip install weaverstack`.
    - Ensure installation works cleanly within Microsoft Fabric environments.

## Developer and user experience

11. **Build a Weaver GUI over the catalogue**
    - Run as a Fabric custom web application.
    - Provide workspace, item, document, dependency, build-status and catalogue views.

12. **Generate HTML documentation into `docs/`**
    - Publish through GitHub Pages.
    - Include repository structure, Weaver documents, bindings, aliases, build behaviour, ETL and catalogue concepts.

13. **Create built-in examples**
    - A minimal single-item example.
    - A realistic multi-item example covering aliases, dependencies, ETL, bookmarks and multiple Fabric target types.

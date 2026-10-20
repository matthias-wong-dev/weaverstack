# Weaverstack

**Making Fabric easier to use.**

Keep writing Python, SQL and native model definitions. Weaver handles the
installation, load work and recorded results around them. Start with the item
you are working on, or combine several: dependencies connect the work, and
Weaver builds, loads, tests and tracks its state across them.

- [Lakehouse](https://docs.weaverstack.dev/lakehouse/): turn files and Python or Spark SQL into managed Tables.
- [Warehouse](https://docs.weaverstack.dev/warehouse/): write a SELECT and let Weaver build the Table and its load machinery.
- [Semantic models](https://docs.weaverstack.dev/semantic-models/): deploy native TMDL, then add modelling conventions and DAX checks.
- [Reports](https://docs.weaverstack.dev/reports/): deploy native Reports with the right model in each workspace.

New to Weaver? [Start here](https://docs.weaverstack.dev/getting-started/).

## Installation

Weaver requires Python 3.11 or later and is tested on Python 3.11 and 3.13.
See the [installation guide](https://docs.weaverstack.dev/getting-started/installation/).

## A first project

Create a project with the Sales example, then build it into a Fabric workspace:

```bash
weaver initialise \
  --workspace Analytics \
  --project-folder ./analytics \
  --lakehouse Landing \
  --example Sales \
  --non-interactive
cd analytics
weaver fabric environment publish --path Environment/Weaver.Environment
weaver build
```

## Documentation

- [Documentation](https://docs.weaverstack.dev): guides and the complete command and API reference
- [Website](https://weaverstack.dev): what Weaver does
- [Source](https://github.com/matthias-wong-dev/weaverstack)

## Licence

Mozilla Public License 2.0. See [LICENSE](https://github.com/matthias-wong-dev/weaverstack/blob/main/LICENSE).

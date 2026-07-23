# Using the Weaver CLI

```bash
pip install 'weaverstack[cli]'
weaver --help
```

Everything below is optional depending on what you are doing. Working against a
local filesystem needs no Azure at all; working against Fabric needs `az login`
and nothing else.

---

## Signing in to Azure

Weaver uses whatever identity the Azure CLI is signed in as. There is no Weaver
credential, no service principal to register, no secret to store.

```bash
brew install azure-cli
az login
```

That opens a browser once. Afterwards:

```bash
az account show
```

should name your subscription and tenant. If it names the wrong subscription:

```bash
az account set --subscription "<name or id>"
```

### Why Weaver pins the credential

Under the hood this is `DefaultAzureCredential`, which walks a chain of possible
identities — environment variables, managed identity, the Azure CLI, and others
— and uses the first that answers. The chain does not always settle on the one
you signed in as, and the symptom is confusing: ARM calls and `az` both work,
but a OneLake write fails with `401 Access token validation failed`.

So Weaver sets `AZURE_TOKEN_CREDENTIALS=AzureCliCredential` when nothing else
has, which makes the identity the same one `az account show` reports. If you set
that variable yourself, your choice stands and Weaver does not touch it.

If a call still returns 401, refresh the CLI's cache for the storage audience:

```bash
az account get-access-token --resource https://storage.azure.com/
```

---

## Naming where the work happens

Every command that touches something takes a **host** — the workspace or local
root the work happens in.

```bash
--host MyFabric --hosts env.yml    # a host named in a hosts file
--root .local                      # a local host, no file needed
```

A hosts file is a convenience, never a requirement — see
[`examples/env.yml`](../examples/env.yml). It holds only level four, the
workspace or root. Lakehouses, Warehouses and Environments are named directly
wherever they are used, because their names are already unique inside a
workspace.

```yaml
hosts:
  MyLocal:
    type: Local
    root: .local
    weaver_lakehouse: Weaver

  MyFabric:
    type: Fabric
    workspace: Weaver
    weaver_lakehouse: Weaver
```

---

## Capacity

A Fabric capacity is billed while it runs, so a session starts and ends with it.

```bash
weaver capacity resume  --resource-group <rg> --capacity-name <capacity>
weaver capacity status  --resource-group <rg> --capacity-name <capacity>
weaver capacity suspend --resource-group <rg> --capacity-name <capacity>
```

```text
datawithoutguessing: Active, F2
```

`resume` returns before the capacity is actually `Active` — it takes about half
a minute — so `status` is the confirmation, not the return value.

Find your capacity with:

```bash
az fabric capacity list --query "[].{name:name, rg:resourceGroup}" -o table
```

---

## Wipe

Clears a target. **It removes everything there, not only what Weaver
manages**, so it prints the plan first and asks before acting.

```bash
weaver wipe --target Sales_LH --host MyLocal --hosts env.yml --dry-run
```

```text
wipe on MyLocal

  folder:Sales_LH/Files
    .local/Sales_LH/Files
      - Sales
  delta:Sales_LH
    .local/Sales_LH/Tables
      - Sales

2 item(s) would be removed. Nothing was changed.
```

The target's *shape* says what it is. A bare name is an item and clears all of
it; a path narrows to one folder root:

```bash
weaver wipe --target Sales_LH                     # the whole Lakehouse
weaver wipe --target Sales_LH/Files/Extracts      # only that folder root
weaver wipe --target A --target B                 # several, repeat the flag
```

Add `--yes` to skip the confirmation. Without a terminal to ask on — in a script
or CI — the command **refuses** rather than proceeding, so nothing is destroyed
by omission.

---

## Checking a machine

```bash
weaver doctor
```

Reports whether local Spark and Delta will work: Python, PySpark, delta-spark
and Java, each failure naming the command that fixes it. `--json` for scripting;
the exit status is non-zero when something is missing.

None of it is needed to use Weaver on Fabric — it is for local development.

---

## A session, end to end

```bash
az login

weaver capacity resume --resource-group <rg> --capacity-name <capacity>
weaver capacity status --resource-group <rg> --capacity-name <capacity>

# … work …

weaver capacity suspend --resource-group <rg> --capacity-name <capacity>
```

## See also

- [Where your SES repository lives](ses-repository.md) — delivery routes, all optional

- [Local development setup](local-setup.md) — Java, Spark, and the local tests
- [Fabric integration tests](fabric-testing.md) — running the opt-in suite

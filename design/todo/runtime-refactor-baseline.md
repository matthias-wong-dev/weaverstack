# Runtime architecture refactor — Phase 0 baseline

Recorded on `agent/runtime-architecture-refactor` at `df09551`, before any
behaviour changed, so the Phase 9 acceptance compares like with like. The
headline number matters less than the shape: what the slow list is made of.

## Totals

```text
pure       2059 passed     1m 25s
spark       247 passed    11m 55s
fabric      82 passed     21m 10s   (-m "fabric and remote")
```

`fabric and hosted` (30 tests) is not baselined here: it runs the *published*
wheel, so its number is only meaningful against a publish, and the branch
changes what crosses to the wheel.

## Where the Fabric time goes

The top of `--durations` is dominated by two files, and both are doing real
Warehouse work:

```text
114.0s  setup  test_cross_item_alias.py         (one estate, built once)
 58.7s  call   test_warehouse_load_primitive.py  second run updates only changed
 56.8s  call   test_warehouse_load_primitive.py  unchanged row keeps update time
 52.2s  call   test_warehouse_load_primitive.py  non-incremental deletes
 46.4s  call   test_warehouse_load_primitive.py  intolerant run with rejects
 44.4s  call   test_warehouse_load_primitive.py  generated identities
 ~30s   ×14    test_warehouse_{load,sql_program}_primitive.py
```

Two observations the refactor acts on:

- the second and third entries are **two assertions about one expensive second
  run**, paid for twice. §18.5 consolidates these.
- the `~30s` band is a function-scoped baseline that installs and first-loads the
  same object per case. §17 makes reuse the default.

## Where the Spark time goes

```text
 2059 pure tests cost 85s in total
  247 spark tests cost 715s
```

The pure suite is not the problem and is not the target. The Spark suite is,
because much of what it proves is Python orchestration that pays for a JVM and a
built estate to ask a question neither answers.

## Method

```bash
pytest -q --durations=25
pytest -m spark -q --durations=25
pytest -m "fabric and remote" -q --durations=25
```

Same commands, same machine, at the end of the branch.

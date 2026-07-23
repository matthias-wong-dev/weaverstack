# Local development on macOS

Everything Weaver does on Fabric, it can also do against a local filesystem
standing in for Lakehouses. That is optional — the core installs and imports
without any of this — but it is how build and load are developed and tested
without touching a workspace.

## What you need

| | version | why |
|---|---|---|
| Python | 3.11 or later | the package baseline |
| A JDK | **11 or 17** | Spark runs on the JVM |
| PySpark | 3.5.x | |
| delta-spark | 3.2.x | Delta and Spark are released in lockstep |

Spark 3.5 does **not** support Java 21 or later. If `java -version` reports one
of those, install a supported JDK alongside it and point `JAVA_HOME` at that;
nothing needs uninstalling.

## Setting it up

```bash
brew install openjdk@17 python@3.11
```

```bash
cd weaverstack
python3.11 -m venv .venv
.venv/bin/pip install -e '.[dev]'
```

Then check the machine rather than guessing:

```bash
.venv/bin/weaver doctor
```

```text
local Spark and Delta on Darwin arm64

  ok       python         3.11.15
  ok       pyspark        3.5.1
  ok       delta-spark    3.2.0
  ok       java           17.0.19 (/opt/homebrew/Cellar/openjdk@17/…)

Ready. Run the local tests with:  pytest -m spark
```

Anything missing is named with the command that fixes it, and the exit status
is non-zero so it can gate a script.

`JAVA_HOME` does not need setting by hand. When it is unset, Weaver asks
`/usr/libexec/java_home` for a supported JDK, newest first. When it *is* set,
it is respected — a deliberately configured machine is never second-guessed.

## Running the tests

```bash
.venv/bin/python -m pytest              # core only, no JVM, under a second
.venv/bin/python -m pytest -m spark     # local Spark and Delta
```

Spark tests are deselected by default and skip themselves when PySpark or a JDK
is absent, so a contributor without a JVM is never blocked and CI needs no
special casing.

## Why the fixtures are scoped as they are

Measured on an M-series Mac:

| | cost |
|---|---|
| Spark session start | 1.24 s |
| first Delta write and read (JVM warm-up) | 4.31 s |
| later Delta write and read | ~0.75 s |
| a local Lakehouse skeleton | 0.0002 s |

So `spark` is **session-scoped** — built once for the whole run — and
`lakehouses` is **per-test**. Only one `SparkSession` may be active in a process
anyway, and the warm-up is not worth paying twice. Lakehouse directories are
free enough that reusing them would only invite cross-test contamination.

Sharing a session is safe because Weaver addresses Delta by explicit path rather
than through a metastore, so no catalogue state accumulates between tests.
Isolation comes from each test's own `tmp_path`.

## If a Spark test fails oddly

Almost always one of two things:

**A Python version mismatch inside a task.** Spark launches workers with
`PYSPARK_PYTHON`, which defaults to whatever `python3` resolves to — often the
system interpreter rather than your virtualenv. The fixture pins it to
`sys.executable`, so this should not happen here; it will happen in a script of
your own that builds its own session.

**An unsupported JDK.** `weaver doctor` names the version it found.

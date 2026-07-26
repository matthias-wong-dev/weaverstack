# Local development

Everything Weaver does on Fabric, it can also do against a local filesystem
standing in for Lakehouses. That is optional — the core installs and imports
without any of this — but it is how build and load are developed and tested
without touching a workspace.

Weaver is developed on macOS and tested on macOS, Linux and Windows. The core
runs on all three; local Spark runs on macOS and Linux, and on Windows through
WSL — see [Windows](#windows) below.

## What you need

| | version | why |
|---|---|---|
| Python | 3.11 or later | the package baseline |
| A JDK | **11 or 17**, or 21 | Spark runs on the JVM |
| PySpark | 3.5.x | |
| delta-spark | 3.2.x | Delta and Spark are released in lockstep |

Spark 3.5 documents Java 8, 11 and 17. Java 21 is undocumented but runs the
local suite, so `weaver doctor` accepts it — a machine that only ships 21 is not
blocked. It is accepted, not preferred: where several are installed, Weaver
still picks a documented release first.

## Setting it up

Install a JDK and Python 3.11 or later.

**macOS**

```bash
brew install openjdk@17 python@3.11
```

**Linux** (Debian or Ubuntu; use your distribution's equivalent elsewhere)

```bash
sudo apt install openjdk-17-jdk python3.11 python3.11-venv
```

**Windows**

```powershell
winget install Microsoft.OpenJDK.17
winget install Python.Python.3.11
```

Then, on any of them:

```bash
cd weaverstack
python3.11 -m venv .venv
.venv/bin/pip install -e '.[dev]'      # Windows: .venv\Scripts\pip
```

Install **editable** (`-e`). `weaver install` finds the checkout by walking up
from the installed package to its `pyproject.toml`, so a plain copy into a
site-packages directory outside the tree cannot locate it.

Which extra:

| | installs | for |
|---|---|---|
| `.[test]` | the suite, no JVM | core tests, and [Fabric tests](fabric-testing.md) against a workspace |
| `.[dev]` | `[test]` plus PySpark and Delta | local Spark work as well |
| `.[cli]` | the desktop CLI | `weaver install`, `weaver capacity` |

`[dev]` is a few hundred megabytes and PySpark builds from source, so take
`[test]` if you are not doing local Spark. `requirements-dev.txt` in the root is
a pinned set for CI only — you never need it to install, use or test Weaver.

If `pip install` fails building PySpark with `AttributeError: install_layout`,
the interpreter is a distribution-patched Python whose bundled setuptools cannot
build PySpark's source distribution. A virtual environment with current
packaging tools builds it:

```bash
.venv/bin/pip install -U pip setuptools wheel
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

Anything missing is named with the command that fixes it — in this platform's
package manager, not another's — and the exit status is non-zero so it can gate
a script.

`JAVA_HOME` does not need setting by hand. When it is unset, Weaver asks
`/usr/libexec/java_home` for a supported JDK on macOS, newest first, and falls
back to whatever `java` is on `PATH` elsewhere. When it *is* set, it is
respected — a deliberately configured machine is never second-guessed.

## Windows

The core suite runs natively and CI covers it on every push, across Python 3.11
and 3.12. The CLI, the catalogue, the dependency graph, SQL generation and
Warehouse targets all work without anything special.

Local **Spark** does not: Spark writes to the local filesystem through Hadoop's
native IO, which needs a `winutils.exe` and a matching `HADOOP_HOME`. That is a
Spark-on-Windows limitation rather than a Weaver one. For local Spark work, use
[WSL](https://learn.microsoft.com/windows/wsl/install) and follow the Linux
instructions inside it.

One thing to know if you are working on Weaver itself: a `Location` normalises
`\` to `/` at construction, so its value is POSIX whatever the platform. Every
consumer — `join`, `name`, the SES reader's segment splitting — assumes a single
separator, and `LocalHost` normalises its root through `Path`, which on Windows
yields backslashes. Read paths back through `Location`, not by string surgery.

## Running the tests

```bash
.venv/bin/python -m pytest              # core only, no JVM, under a second
.venv/bin/python -m pytest -m spark     # local Spark and Delta
```

Spark tests are deselected by default and skip themselves when PySpark or a JDK
is absent, so a contributor without a JVM is never blocked.

CI runs both: the core suite on macOS, Linux and Windows across Python 3.11 and
3.12, and the Spark suite on macOS and Linux across Java 17 and 21. See
[.github/workflows/tests.yml](../.github/workflows/tests.yml). Fabric tests need
a workspace and a running capacity, so they stay opt-in and local — see
[fabric-testing.md](fabric-testing.md).

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

Isolation comes from each test's own `tmp_path`, and one shared session needs help
with that. Delta caches a `DeltaLog` per table **path** — and through it a
`Snapshot`, a query execution and its encoder — so a suite that builds every table
under a fresh directory keeps the retained state of every Lakehouse it has already
deleted. Measured, that is about 5.6 MB of live heap per test, which exhausted the
default 1 GB driver heap partway through the run. An autouse fixture clears Delta's
log cache and Spark's plan cache after each test; both are caches, so the cost is
re-reading a transaction log.

A test that registers a *schema* still has to drop it, because a schema is not a
cache: two tests present different temporary directories under the same logical
Lakehouse name, and a schema left registered would send the second test's tables
into the first test's directory.

## If a Spark test fails oddly

Almost always one of three things:

**The heap.** If a long `-m spark` run starts failing late with a
`Py4JJavaError` whose message is `<exception str() failed>`, or attributes a
failure to a test that only starts a session, the driver has run out of heap and
the named test is a bystander. Check that whatever built a session did not bypass
the shared `spark` fixture and its cache release.

**A Python version mismatch inside a task.** Spark launches workers with
`PYSPARK_PYTHON`, which defaults to whatever `python3` resolves to — often the
system interpreter rather than your virtualenv. The fixture pins it to
`sys.executable`, so this should not happen here; it will happen in a script of
your own that builds its own session.

**An unsupported JDK.** `weaver doctor` names the version it found.

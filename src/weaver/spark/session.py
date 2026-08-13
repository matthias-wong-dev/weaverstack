"""A leak-free local Delta session for CLI execution."""

from __future__ import annotations

import os
import sys
from importlib import import_module
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from ..diagnostics import SUPPORTED_JAVA, find_java_home
from ..errors import CommandError


#: Spark's identifier-analysis setting.
CASE_SENSITIVE = "spark.sql.caseSensitive"


def apply_emulator_analysis_policy(spark) -> None:
    """Make an emulator session analyse identifiers exactly, for its whole life.

    Every declared object keeps its Weaver spelling, and the emulator's folded
    schema names are lower case. Unlike Fabric's catalogue, Spark's local session
    catalogue cannot look a PascalCase table up again once analysis returns to
    case-insensitive, so this is a session policy rather than a scope around one
    statement.
    """

    spark.conf.set(CASE_SENSITIVE, "true")


@contextmanager
def local_delta_session(workspace=None) -> Iterator[object]:
    """Create one local Delta session and always stop it before returning.

    CLI invocations are separate JVMs. When a local Workspace is supplied its
    Spark metastore therefore lives beneath that emulator root, so namespaces
    and table registrations created by ``initialise`` remain visible to the next
    ``build`` command just as Fabric's catalogue remains visible between
    sessions. Tests that supply no Workspace keep Spark's process-local default.
    """

    try:
        configure_spark_with_delta_pip = import_module(
            "delta"
        ).configure_spark_with_delta_pip
        SparkSession = import_module("pyspark.sql").SparkSession
    except ImportError as exc:
        raise CommandError(
            "local build needs the Spark extra; install weaverstack[spark]"
        ) from exc

    java_home = find_java_home()
    if java_home is None:
        raise CommandError(
            f"local build needs Java {' or '.join(SUPPORTED_JAVA)}; run weaver doctor"
        )
    previous = {
        name: os.environ.get(name)
        for name in ("JAVA_HOME", "PYSPARK_PYTHON", "PYSPARK_DRIVER_PYTHON")
    }
    os.environ["JAVA_HOME"] = java_home
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    builder = (
        SparkSession.builder.appName("weaverstack-cli")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.databricks.delta.snapshotPartitions", "1")
    )
    if workspace is not None:
        root_value = getattr(workspace, "workspace", workspace)
        root = Path(root_value).expanduser().resolve() / ".weaver" / "spark"
        builder = (
            builder.config("spark.sql.catalogImplementation", "hive")
            .config("spark.sql.warehouse.dir", str(root / "warehouse"))
            .config(
                "javax.jdo.option.ConnectionURL",
                f"jdbc:derby:;databaseName={root / 'metastore'};create=true",
            )
        )
    session = None
    try:
        session = configure_spark_with_delta_pip(builder).getOrCreate()
        session.sparkContext.setLogLevel("ERROR")
        apply_emulator_analysis_policy(session)
        yield session
    finally:
        if session is not None:
            session.stop()
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

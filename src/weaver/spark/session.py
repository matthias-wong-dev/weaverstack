"""A leak-free local Delta session for CLI execution."""

from __future__ import annotations

import os
import sys
from importlib import import_module
from contextlib import contextmanager
from typing import Iterator

from ..diagnostics import SUPPORTED_JAVA, find_java_home
from ..errors import CommandError


@contextmanager
def local_delta_session() -> Iterator[object]:
    """Create one local Delta session and always stop it before returning."""

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
    session = None
    try:
        session = configure_spark_with_delta_pip(builder).getOrCreate()
        session.sparkContext.setLogLevel("ERROR")
        yield session
    finally:
        if session is not None:
            session.stop()
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Chemin vers le dossier SRC
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "SRC"

if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))


@pytest.fixture(scope="session")
def spark():
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("pytest-sonar")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )

    # Charge les bibliothèques Delta avant la création de Spark
    session = configure_spark_with_delta_pip(builder).getOrCreate()

    yield session

    session.stop()


@pytest.fixture(autouse=True)
def inject_spark(request, spark):
    request.module.spark = spark
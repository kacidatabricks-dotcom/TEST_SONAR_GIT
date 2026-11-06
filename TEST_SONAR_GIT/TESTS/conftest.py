
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("pytest-sonar")
        .getOrCreate()
    )

    yield session

    session.stop()

'''
import os
import sys
import pytest

from pathlib import Path
from pyspark.sql import SparkSession

# =====================================================
# CHEMINS DU PROJET
# =====================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "SRC"

if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))


# =====================================================
# INITIALISATION DE SPARK
# =====================================================

@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("SonarCloud-PySpark-Tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    yield session

    session.stop()


# =====================================================
# MISE A DISPOSITION DE SPARK DANS LES TESTS
# =====================================================

@pytest.fixture(autouse=True)
def inject_spark(request, spark):
    """
    Rend la session Spark accessible aux tests
    qui utilisent directement la variable globale spark.
    """

    request.module.spark = spark

'''

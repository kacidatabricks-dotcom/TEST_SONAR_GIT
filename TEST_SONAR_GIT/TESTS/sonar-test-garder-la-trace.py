# Ajout des données pour avoir la trace

from pyspark.sql import functions as F

import importlib
ajoutmétadonnées = importlib.import_module("sonar-fonc-garder-la-trace").ajoutmétadonnées

def test_ajoutmétadonnées():
    test_data = [("123",), ("456",)]
    columns = ["AB_CLIENT_ID"]
    df_test = spark.createDataFrame(test_data, columns)
    result_df = ajoutmétadonnées(df_test, "Oracle", "Oracle_clients.csv", "AB_CLIENT_ID")
    assert "SOURCE_SYSTEM" in result_df.columns
    assert "ORIGINAL_ID" in result_df.columns
    assert "SOURCE_FILE" in result_df.columns
    assert "INGESTION_TIMESTAMP" in result_df.columns
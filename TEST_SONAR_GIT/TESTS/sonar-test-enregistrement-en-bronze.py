import importlib
enregistrement_bronze = importlib.import_module("sonar-fonc-enregistrement-en-bronze").enregistrement_bronze

def test_enregistrement_bronze():
    # Création d'un DataFrame de test
    data = [("1", "Alice"), ("2", "Bob")]
    columns = ["id", "name"]
    df_test = spark.createDataFrame(data, columns)
    test_tablename = "bronze.test_table"

    # Appel de la fonction
    enregistrement_bronze(df_test, test_tablename)

    # Vérifie que la table a été créée et contient les bonnes données
    df_loaded = spark.table(test_tablename)
    assert df_loaded.count() == 2
    assert set(df_loaded.columns) == set(columns)
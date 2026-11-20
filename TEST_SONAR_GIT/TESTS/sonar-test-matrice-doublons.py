# Étape 3 : Matrice de Doublons et Création de la Dimension Client

# Algorithme d'identification des doublons (Matching exact sur Nom, Prénom, Date de Naissance ou Téléphone)
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, upper, initcap
import importlib
generer_matrice_doublons = importlib.import_module("sonar-fonc-matrice-doublons").generer_matrice_doublons

def test_generer_matrice_doublons():
    from pyspark.sql import Row
    test_data = [
        Row(NOM="Dupont", PRENOM="Jean", DATE_NAISSANCE="1980-01-01", ORIGINAL_ID=1),
        Row(NOM="Dupont", PRENOM="Jean", DATE_NAISSANCE="1980-01-01", ORIGINAL_ID=2),
        Row(NOM="Martin", PRENOM="Pierre", DATE_NAISSANCE="1990-02-02", ORIGINAL_ID=3)
    ]
    df_test = spark.createDataFrame(test_data)
    matched_clients, dim_clients, doublons_df, df_result = generer_matrice_doublons(df_test)
    df_result.show(truncate=False)
    assert df_result.count() == 2, "La matrice de doublons devrait contenir 2 lignes"
    #assert df_result.filter(col("NB_OCCURENCES") == 2).count() == 1


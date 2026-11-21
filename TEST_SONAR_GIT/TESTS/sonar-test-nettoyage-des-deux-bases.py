
#test_nettoiage_des_deux_bases(df_clients)

import importlib
_mod = importlib.import_module("sonar-fonc-nettoyage-des-deux-bases")
data_cleaning = _mod.data_cleaning

create_ab_dataframes = importlib.import_module("sonar-fonc-ingestion-des-donnees-oracle").create_ab_dataframes
load_ap_data = importlib.import_module("sonar-fonc-ingestion-des-donnees-assureplus").load_ap_data
from pyspark.sql import functions as F


def test_nettoyage_des_deux_bases(spark):


    # L'ingestion lit les CSV réels de DATA/SAMPLE (ABASSURANCE et APASSURANCE)
    # du dépôt : sans data_dir, chaque fonction retombe sur son dossier par défaut.
    df1_test, _, _, _ = create_ab_dataframes(spark)
    df2_test, _, _, _ = load_ap_data(spark)

    df1_cleaned, df2_cleaned = data_cleaning(df1_test, df2_test)

    # Les deux bases nettoyées ne doivent pas être vides
    assert df1_cleaned.count() > 0
    assert df2_cleaned.count() > 0

    # Invariants de normalisation, indépendants de l'ordre des lignes
    # (les valeurs null sont ignorées par les comparaisons) :
    # - noms en majuscules et sans espaces parasites
    # - emails en minuscules
    assert df1_cleaned.filter(F.col("AB_NOM") != F.upper(F.trim(F.col("AB_NOM")))).count() == 0
    assert df1_cleaned.filter(F.col("AB_EMAIL") != F.lower(F.trim(F.col("AB_EMAIL")))).count() == 0
    assert df2_cleaned.filter(F.col("AP_NOM") != F.upper(F.trim(F.col("AP_NOM")))).count() == 0
    assert df2_cleaned.filter(F.col("AP_MAIL_ADDRESS") != F.lower(F.trim(F.col("AP_MAIL_ADDRESS")))).count() == 0


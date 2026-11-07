"""
refaire le test pour prendre en compte les deux dataframes et exécuter les fonctions définies
# Étape 2 : Data Pipeline - Harmonisation & Unification (Silver Layer)

Schéma client final :

MASTER_CLIENT_ID
SOURCE_SYSTEM
SOURCE_CLIENT_ID
NOM
PRENOM
NOM_COMPLET
DATE_NAISSANCE
EMAIL
TELEPHONE
ADRESSE
CODE_POSTAL
NUMERO_FISCAL
DATE_CREATION
STATUT_CLIENT
SCORE_FIDELITE
DATE_INTEGRATION
SOURCE_FILE
INGESTION_TIMESTAMP
VILLE     je ne l'ai pas rajouté



"""



# Exemple d'utilisation :  INTERESSANT
#test_nettoiage_des_deux_bases(df_clients)

from SONAR_FONC_NETTOYAGE_DES_DEUX_BASES import clean_last_name, normalize_firstname, normalize_email, normalize_date, normalize_phone_number, data_cleaning

from SONAR_FONC_INGESTION_DONNEES_ORACLE import create_ab_dataframes
from SONAR_FONC_INGESTION_DES_DONNEES_ASSUREPLUS import load_ap_data

def test_nettoyage_des_deux_bases():

    from pyspark.sql import Row
'''
    df_raw = spark.createDataFrame([
        Row(
            NOM=" dupont ",
            PRENOM="jean",
            EMAIL="JEAN@EXEMPLE.COM",
            ADRESSE=" 1 rue du test ",
            CODE_POSTAL="75000",
            DATE_NAISSANCE="1980-01-01",
            DATE_CREATION="2020-01-01",
            TELEPHONE="0612345678",
            SOURCE_FILE="test.csv",
            VILLE="Paris"
        )
    ])

   # result = test_nettoyage_des_deux_bases(spark)
'''
df1_test,df1_contrats, df1_sinistres, df1_paiements =create_ab_dataframes(spark)
df2_test,df2_contrats, df2_sinistres, df2_paiements =load_ap_data(spark)
df1_test_cleaned, df2_test_cleaned = data_cleaning(df1_test,df2_test)


assert df1_test_cleaned.count() == 1
assert df1_test_cleaned.first()["AB_NOM"] == "MARTIN"
assert df1_test_cleaned.first()["AB_PRENOM"] == "JEAN"
assert df1_test_cleaned.first()["AB_EMAIL"] == "jean.martin@gmail.fr"
assert df1_test_cleaned.first()["AB_TELEPHONE"] == "+33612345678"


assert df2_test_cleaned.count() == 1
assert df2_test_cleaned.first()["AP_NOM"] == "MARTIN"
assert df2_test_cleaned.first()["AP_PRENOM"] == "JEAN"
assert df2_test_cleaned.first()["AP_MAIL_ADDRESS"] == "jean.martin@gmail.fr"
assert df2_test_cleaned.first()["AP_PHONE_NUMBER"] == "+33712345678"


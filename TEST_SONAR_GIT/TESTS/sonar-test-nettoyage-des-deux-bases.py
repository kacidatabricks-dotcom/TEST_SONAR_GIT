
#test_nettoiage_des_deux_bases(df_clients)

import importlib
_mod = importlib.import_module("sonar-fonc-nettoyage-des-deux-bases")
clean_last_name = _mod.clean_last_name
normalize_firstname = _mod.normalize_firstname
normalize_email = _mod.normalize_email
normalize_date = _mod.normalize_date
normalize_phone_number = _mod.normalize_phone_number
data_cleaning = _mod.data_cleaning

create_ab_dataframes = importlib.import_module("sonar-fonc-ingestion-des-donnees-oracle").create_ab_dataframes
load_ap_data = importlib.import_module("sonar-fonc-ingestion-des-donnees-assureplus").load_ap_data
from pyspark.sql import Row

def test_nettoyage_des_deux_bases():


 df1_test,df1_contrats, df1_sinistres, df1_paiements =create_ab_dataframes(spark,data_dir)
 df2_test,df2_contrats, df2_sinistres, df2_paiements =load_ap_data(spark,data_dir)
 df1_test_cleaned, df2_test_cleaned = data_cleaning(df1_test,df2_test)


 assert df1_test_cleaned.count() == 10
 assert df1_test_cleaned.first()["AB_NOM"] == "MARTIN"
 assert df1_test_cleaned.first()["AB_PRENOM"] == "JEAN"
 assert df1_test_cleaned.first()["AB_EMAIL"] == "jean.martin@gmail.fr"
 assert df1_test_cleaned.first()["AB_TELEPHONE"] == "+33612345678"


 assert df2_test_cleaned.count() == 10
 assert df2_test_cleaned.first()["AP_NOM"] == ""
 assert df2_test_cleaned.first()["AP_PRENOM"] == ""
 assert df2_test_cleaned.first()["AP_MAIL_ADDRESS"] == "jean.martin@gmail.fr"
 assert df2_test_cleaned.first()["AP_PHONE_NUMBER"] == "+33712345678"


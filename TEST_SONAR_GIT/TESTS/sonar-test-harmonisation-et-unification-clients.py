import importlib
harmonize_clients = importlib.import_module("sonar-fonc-harmonisation-et-unification-clients").harmonize_clients
# 

def test_harmonize_clients(spark):
    # Création de DataFrames de test avec deux lignes chacun
    from pyspark.sql.functions import lit
    ab_test_data = [
        ("1", "DUPONT", "jean", "1980-01-01", "JEAN.DUPONT@EXEMPLE.COM", "0612345678", "1 rue du Test", "75000", "AB123", "2020-01-01", "Actif", "2026-10-03", "file_ab.csv"),
        ("2", "Durand", "marie", "1990-02-02", "MARIE.DURAND@EXEMPLE.COM", "0698765432", "2 avenue du Test", "69000", "AB456", "2021-01-01", "Inactif", "2026-10-03", "file_ab.csv"),
    ]
    ap_test_data = [
        ("10", "MARTIN", "luc", "1975-05-05", "LUC.MARTIN@EXEMPLE.COM", "0600001111", "10 place du Test", "31000", "AP789", "2019-12-12", "Actif", "2026-10-03", "file_ap.csv"),
        ("20", "BOURSE", "julie", "1985-07-07", "JULIE.BOURSE@EXEMPLE.COM", "0600002222", "20 impasse du Test", "33000", "AP012", "2018-11-11", "Inactif", "2026-10-03", "file_ap.csv"),
    ]
    ab_schema = ["AB_CLIENT_ID", "AB_NOM", "AB_PRENOM", "AB_DATE_NAISSANCE", "AB_EMAIL", "AB_TELEPHONE", "AB_ADRESSE", "AB_CODE_POSTAL", "AB_NUM_FISCAL", "AB_DATE_CREATION", "AB_STATUT_CLIENT", "INGESTION_TIMESTAMP", "SOURCE_FILE"]
    ap_schema = ["AP_USER_ID", "AP_NOM", "AP_PRENOM", "AP_BIRTH_DATE", "AP_MAIL_ADDRESS", "AP_PHONE_NUMBER", "AP_STREET_ADDRESS", "AP_ZIP_CODE", "AP_NUM_FISCAL", "AP_DATE_CREATION", "AP_CUSTOMER_STATUS", "INGESTION_TIMESTAMP", "SOURCE_FILE"]
    df_ab_clients = spark.createDataFrame(ab_test_data, ab_schema)
    df_ap_clients = spark.createDataFrame(ap_test_data, ap_schema)
    df_clients = harmonize_clients(df_ab_clients, df_ap_clients)
    df_clients.show(truncate=False)
    
    # ecrire des assertions pour vérifier que les colonnes ont été renommées et que les valeurs ont été correctement transformées
    assert df_clients.columns == ["SOURCE_SYSTEM", "ORIGINAL_ID", "NOM", "PRENOM", "DATE_NAISSANCE", "EMAIL", "TELEPHONE", "ADRESSE", "CODE_POSTAL", "NUMERO_FISCAL", "DATE_CREATION", "STATUT_CLIENT", "INGESTION_TIMESTAMP", "SOURCE_FILE"]
   # assert df_clients.filter(df_clients.ORIGINAL_ID == "1").select("NOM", "PRENOM", "DATE_NAISSANCE", "EMAIL", "TELEPHONE", "ADRESSE", "CODE_POSTAL", "NUMERO_FISCAL", "DATE_CREATION", "STATUT_CLIENT").collect() == [("DUPONT", "Jean", "1980-01-01", "jean.dupont@example.com", "0612345678", "1 rue du Test", "75000", "AB123", "2020-01-01", "Actif")]
   # assert df_clients.filter(df_clients.ORIGINAL_ID == "10").select("NOM", "PRENOM", "DATE_NAISSANCE", "EMAIL", "TELEPHONE", "ADRESSE", "CODE_POSTAL", "NUMERO_FISCAL", "DATE_CREATION", "STATUT_CLIENT").collect() == [("MARTIN", "Luc", "1975-05-05", "luc.martin@example.com", "0600001111", "10 place du Test", "31000", "AP789", "2019-12-12", "Actif")]
    # assert df_clients.filter(df_clients.ORIGINAL_ID == "2").select("NOM", "PRENOM", "DATE_NAISSANCE", "EMAIL", "TELEPHONE", "ADRESSE", "CODE_POSTAL", "NUMERO_FISCAL", "DATE_CREATION", "STATUT_CLIENT").collect() == [("Durand", "Marie", "1990-02-02", "marie.durand@example.com", "0698765432", "2 avenue du Test", "69000", "AB456", "2021-01-01", "Inactif")]
    # assert df_clients.filter(df_clients.ORIGINAL_ID == "20").select("NOM", "PRENOM", "DATE_NAISSANCE", "EMAIL", "TELEPHONE", "ADRESSE", "CODE_POSTAL", "NUMERO_FISCAL", "DATE_CREATION", "STATUT_CLIENT").collect() == [("BOURSE", "Julie", "1985-07-07", "julie.bourse@example.com", "0600002222", "20 impasse du Test", "33000", "AP012", "2018-11-11", "Inactif")]
    # assert df_clients.filter(df_clients.ORIGINAL_ID == "1").select("SOURCE_SYSTEM", "SOURCE_FILE").collect() == [("SQL", "file_ab.csv")]
    # assert df_clients.filter(df_clients.ORIGINAL_ID == "10").select("SOURCE_SYSTEM", "SOURCE_FILE").collect() == [("ORACLE", "file_ap.csv")]
    

# tahc

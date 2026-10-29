from FONC_HARMONISATION_ET_UNIFICATION_CLIENTS import harmonize_clients
# DBTITLE 1,
"""
# Harmonisation des clients Oracle
std_df_ab_clients = df_ab_clients.select(
    lit("ORACLE").alias("SOURCE_SYSTEM"),
    col("AB_CLIENT_ID").cast("string").alias("ORIGINAL_ID"),
    upper(trim(col("AB_NOM"))).alias("NOM"),
    initcap(trim(col("AB_PRENOM"))).alias("PRENOM"),
    col("AB_DATE_NAISSANCE").alias("DATE_NAISSANCE"),
    lower(trim(col("AB_EMAIL"))).alias("EMAIL"),
    col("AB_TELEPHONE").alias("TELEPHONE"),
    col("AB_ADRESSE").alias("ADRESSE"),
    col("AB_CODE_POSTAL").alias("CODE_POSTAL"),
    col("AB_NUM_FISCAL").alias("NUMERO_FISCAL"),
    col("AB_DATE_CREATION").alias("DATE_CREATION"),
    col("AB_STATUT_CLIENT").alias("STATUT_CLIENT"),
    col("INGESTION_TIMESTAMP").alias("INGESTION_TIMESTAMP"),
    col("SOURCE_FILE").alias("SOURCE_FILE")
)
# Harmonisation des clients SQL Server
std_df_ap_clients = df_ap_clients.select(
    lit("SQL").alias("SOURCE_SYSTEM"),
    col("AP_USER_ID").cast("string").alias("ORIGINAL_ID"),
    upper(trim(col("AP_NOM"))).alias("NOM"),
    initcap(trim(col("AP_PRENOM"))).alias("PRENOM"),
    col("AP_BIRTH_DATE").alias("DATE_NAISSANCE"),
    lower(trim(col("AP_MAIL_ADDRESS"))).alias("EMAIL"),
    col("AP_PHONE_NUMBER").alias("TELEPHONE"),
    col("AP_STREET_ADDRESS").alias("ADRESSE"),
    col("AP_ZIP_CODE").alias("CODE_POSTAL"),
    col("AP_NUM_FISCAL").alias("NUMERO_FISCAL"),
    col("AP_DATE_CREATION").alias("DATE_CREATION"),
    col("AP_CUSTOMER_STATUS").alias("STATUT_CLIENT"),
    col("INGESTION_TIMESTAMP").alias("INGESTION_TIMESTAMP"),
    col("SOURCE_FILE").alias("SOURCE_FILE")
)
# Union des clients harmonisés
df_clients = std_df_ab_clients.union (std_df_ap_clients)
df_clients.createOrReplaceTempView("raw_unified_clients")
df_clients.display()


def harmonize_clients(df_ab_clients, df_ap_clients):
    std_df_ab_clients = df_ab_clients.select(
        lit("ORACLE").alias("SOURCE_SYSTEM"),
        col("AB_CLIENT_ID").cast("string").alias("ORIGINAL_ID"),
        upper(trim(col("AB_NOM"))).alias("NOM"),
        initcap(trim(col("AB_PRENOM"))).alias("PRENOM"),
        col("AB_DATE_NAISSANCE").alias("DATE_NAISSANCE"),
        lower(trim(col("AB_EMAIL"))).alias("EMAIL"),
        col("AB_TELEPHONE").alias("TELEPHONE"),
        col("AB_ADRESSE").alias("ADRESSE"),
        col("AB_CODE_POSTAL").alias("CODE_POSTAL"),
        col("AB_NUM_FISCAL").alias("NUMERO_FISCAL"),
        col("AB_DATE_CREATION").alias("DATE_CREATION"),
        col("AB_STATUT_CLIENT").alias("STATUT_CLIENT"),
        col("INGESTION_TIMESTAMP").alias("INGESTION_TIMESTAMP"),
        col("SOURCE_FILE").alias("SOURCE_FILE")
    )
    std_df_ap_clients = df_ap_clients.select(
        lit("SQL").alias("SOURCE_SYSTEM"),
        col("AP_USER_ID").cast("string").alias("ORIGINAL_ID"),
        upper(trim(col("AP_NOM"))).alias("NOM"),
        initcap(trim(col("AP_PRENOM"))).alias("PRENOM"),
        col("AP_BIRTH_DATE").alias("DATE_NAISSANCE"),
        lower(trim(col("AP_MAIL_ADDRESS"))).alias("EMAIL"),
        col("AP_PHONE_NUMBER").alias("TELEPHONE"),
        col("AP_STREET_ADDRESS").alias("ADRESSE"),
        col("AP_ZIP_CODE").alias("CODE_POSTAL"),
        col("AP_NUM_FISCAL").alias("NUMERO_FISCAL"),
        col("AP_DATE_CREATION").alias("DATE_CREATION"),
        col("AP_CUSTOMER_STATUS").alias("STATUT_CLIENT"),
        col("INGESTION_TIMESTAMP").alias("INGESTION_TIMESTAMP"),
        col("SOURCE_FILE").alias("SOURCE_FILE")
    )
    df_clients = std_df_ab_clients.union(std_df_ap_clients)
    df_clients.createOrReplaceTempView("raw_unified_clients")
    return df_clients
"""
def test_harmonize_clients():
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
    display(df_clients)
    return df_clients

# Exécution du test
test_harmonize_clients()

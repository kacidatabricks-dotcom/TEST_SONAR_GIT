# Ajout des données pour avoir la trace

from pyspark.sql import functions as F
"""
# 1. Définition des fonctions
def ajoutmétadonnées(df, source_system, source_file, client_id_col):
    return df \
        .withColumn("SOURCE_SYSTEM", F.lit(source_system)) \
        .withColumn("ORIGINAL_ID", F.col(client_id_col).cast("string")) \
        .withColumn("SOURCE_FILE", F.lit(source_file)) \
        .withColumn("INGESTION_TIMESTAMP", F.current_timestamp())



# 2. Exécution de la chaîne (Exemple avec un DataFrame initialisé)
# oracle_clients = spark.read...



# ORACLE
df_ab_clients =ajoutmétadonnées (df_ab_clients,"Oracle","Oracle_clients.csv","AB_CLIENT_ID")

#SQL Server
df_ap_clients =ajoutmétadonnées (df_ap_clients,"SQL","SQL_clients.csv","AP_USER_ID")


display(df_ab_clients)
display(df_ap_clients)
"""

def test_ajoutmétadonnées():
    test_data = [("123",), ("456",)]
    columns = ["AB_CLIENT_ID"]
    df_test = spark.createDataFrame(test_data, columns)
    result_df = ajoutmétadonnées(df_test, "Oracle", "Oracle_clients.csv", "AB_CLIENT_ID")
    assert "SOURCE_SYSTEM" in result_df.columns
    assert "ORIGINAL_ID" in result_df.columns
    assert "SOURCE_FILE" in result_df.columns
    assert "INGESTION_TIMESTAMP" in result_df.columns
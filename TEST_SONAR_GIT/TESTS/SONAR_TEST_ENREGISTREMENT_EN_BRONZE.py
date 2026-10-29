
"""
#                                   ENREGISTREMENT EN BRONZE        
# ATTENTION: Toutes les tables ne sont pas enregistrées dans la base de données delta
def enregistrement_bronze(df,tablename):

 df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tablename)

# Tables Oracles
enregistrement_bronze(df_ab_clients,"bronze.oracle_clients")
enregistrement_bronze(df_ab_contrats,"bronze.oracle_contrats")
enregistrement_bronze(df_ab_sinistres,"bronze.oracle_sinistres")
enregistrement_bronze(df_ab_payments,"bronze.oracle_payments")

# Tables SQL Server
enregistrement_bronze(df_ap_clients,"bronze.sqlserver_clients")
enregistrement_bronze(df_ap_contrats,"bronze.sqlserver_contrats")
enregistrement_bronze(df_ap_payments,"bronze.sqlserver_payments")
enregistrement_bronze(df_ap_sinistres,"bronze.sqlserver_sinistres")

"""
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
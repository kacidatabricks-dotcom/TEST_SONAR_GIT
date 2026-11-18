

from   SONAR_FONC_INGESTION_DONNEES_ORACLE  import create_ab_dataframes
def test_create_ab_dataframes(spark):
    dfs = create_ab_dataframes(spark,data_dir)
    # Vérification des DataFrames créés
    # (l'ingestion lit désormais les CSV réels de DATA/SAMPLE/ABASSURANCE,
    #  pas les 10 lignes d'exemple historiques : on vérifie qu'ils sont non vides)
    for i, df in enumerate(dfs):
        assert df.count() > 0, f"Le DataFrame #{i} ne doit pas être vide"
    # Vérification des colonnes clés
    assert "AB_CLIENT_ID" in dfs[0].columns
    assert "AB_POLICY_NUMBER" in dfs[1].columns
    assert "AB_CLAIM_ID" in dfs[2].columns
    assert "AB_PAYMENT_ID" in dfs[3].columns
    # Régression : lecture avec schéma explicite -> AB_TELEPHONE et AB_NUM_FISCAL
    # doivent rester des chaînes (avec inferSchema, le 0 initial du téléphone est
    # perdu et Silver rejette alors quasi tous les clients)
    types_clients = dict(dfs[0].dtypes)
    assert types_clients.get("AB_TELEPHONE") == "string", "AB_TELEPHONE doit être en StringType"
    assert types_clients.get("AB_NUM_FISCAL") == "string", "AB_NUM_FISCAL doit être en StringType"


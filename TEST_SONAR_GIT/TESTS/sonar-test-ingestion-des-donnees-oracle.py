

import importlib
create_ab_dataframes = importlib.import_module("sonar-fonc-ingestion-des-donnees-oracle").create_ab_dataframes
def test_create_ab_dataframes(spark):
    # Sans data_dir, la fonction retombe sur DATA/SAMPLE/ABASSURANCE du dépôt
    dfs = create_ab_dataframes(spark)
    # Vérification des DataFrames créés
    # (l'ingestion lit désormais les CSV réels de DATA/SAMPLE/ABASSURANCE,
    
    for i, df in enumerate(dfs):
        assert df.count() > 0, f"Le DataFrame #{i} ne doit pas être vide"
    # Vérification des colonnes clés
    assert "AB_CLIENT_ID" in dfs[0].columns
    assert "AB_POLICY_NUMBER" in dfs[1].columns
    assert "AB_CLAIM_ID" in dfs[2].columns
    assert "AB_PAYMENT_ID" in dfs[3].columns
   
    types_clients = dict(dfs[0].dtypes)
    assert types_clients.get("AB_TELEPHONE") == "string", "AB_TELEPHONE doit être en StringType"
    assert types_clients.get("AB_NUM_FISCAL") == "string", "AB_NUM_FISCAL doit être en StringType"


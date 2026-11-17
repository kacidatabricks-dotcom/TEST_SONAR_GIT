

from   SONAR_FONC_INGESTION_DONNEES_ORACLE  import create_ab_dataframes
def test_create_ab_dataframes():
    dfs = create_ab_dataframes(spark)
    # Vérification des DataFrames créés
    assert dfs[0].count() == 10  # df_ab_clients
    assert dfs[1].count() == 10  # df_ab_contrats
    assert dfs[2].count() == 10  # df_ab_sinistres
    assert dfs[3].count() == 10  # df_ab_payments
    # Vérification des colonnes clés
    assert "AB_CLIENT_ID" in dfs[0].columns
    assert "AB_POLICY_NUMBER" in dfs[1].columns
    assert "AB_CLAIM_ID" in dfs[2].columns
    assert "AB_PAYMENT_ID" in dfs[3].columns


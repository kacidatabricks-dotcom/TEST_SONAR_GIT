
def test_unify_and_map_contrats():
    # Création des DataFrames de test
    df_ab_contrats = spark.createDataFrame([
        ("P123", "CL001", "AUTO", "ACTIVE", 1000)
    ], ["AB_POLICY_NUMBER", "AB_CLIENT_ID", "AB_TYPE_ASSURANCE", "AB_STATUT_CONTRAT", "AB_PRIME_ANNUELLE"])

    df_ap_contrats = spark.createDataFrame([
        ("C456", "CL002", "HOME", "INACTIVE", 500)
    ], ["AP_CONTRACT_REF", "AP_USER_ID", "AP_PRODUCT_CODE", "AP_CONTRACT_STATE", "AP_MONTHLY_PREMIUM"])

    dim_clients = spark.createDataFrame([
        ("CL001", "ORACLE", "M001"),
        ("CL002", "SQL", "M002")
    ], ["ORIGINAL_ID", "SOURCE_SYSTEM", "MASTER_CLIENT_ID"])

    result = unify_and_map_contrats(df_ab_contrats, df_ap_contrats, dim_clients)
    
    assert result is not None, "Le DataFrame de résultat ne doit pas être None"
    assert result.count() == 2, "Le DataFrame doit contenir deux contrats"
    result_ids = [row.CONTRAT_ID for row in result.collect()]
    assert "P123" in result_ids and "C456" in result_ids, "Les identifiants de contrats doivent être présents"

test_unify_and_map_contrats()
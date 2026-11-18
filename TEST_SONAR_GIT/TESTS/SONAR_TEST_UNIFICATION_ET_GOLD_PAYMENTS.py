# Test unification payments

from SONAR_FONC_UNIFICATION_ET_GOLD_PAYMENT import unify_and_map_payments
def test_unify_payments():
    # Création de dataframes de test
    ab_data = [
        ("1", "PN1", "2024-01-01", 100, "VIREMENT"),
        ("2", "PN2", "2024-01-02", 200, "CHEQUE")
    ]
    ap_data = [
        ("A", "PN1", "2024-01-03", 150, "CB"),
        ("B", "PN3", "2024-01-04", 300, "VIREMENT")
    ]
    ab_schema = ["AB_PAYMENT_ID", "AB_POLICY_NUMBER", "AB_DATE_PAYMENT", "AB_MONTANT", "AB_MODE_PAYMENT"]
    ap_schema = ["AP_PAYMENT_REF", "AP_CONTRACT_REF", "AP_PAYMENT_DATETIME", "AP_AMOUNT_PAID", "AP_PAYMENT_CHANNEL"]
    df_ab_test = spark.createDataFrame(ab_data, ab_schema)
    df_ap_test = spark.createDataFrame(ap_data, ap_schema)

    fact_contrats_test = spark.createDataFrame([("PN1", 1), ("PN2", 2), ("PN3", 3)], ["CONTRAT_ID", "MASTER_CLIENT_ID"])
    result_df = unify_and_map_payments(df_ab_test, df_ap_test, fact_contrats_test, table_name="gold.test_gold_fact_payment")
    result_df.show(truncate=False)
# test unifysinistres

from SONAR_FONC_UNIFICATION_ET_GOLD_SINISTRE import unify_and_map_sinistres
def test_unify_sinistres():
    from pyspark.sql import Row
    test_ab = spark.createDataFrame([Row(AB_CLAIM_ID="1", AB_POLICY_NUMBER="A", AB_MONTANT_ESTIME=100, AB_STATUT_SINISTRE="Ouvert")])
    test_ap = spark.createDataFrame([Row(AP_SINISTRE_NUM="2", AP_CONTRACT_REF="B", AP_ESTIMATED_AMOUNT=200, AP_CLAIM_STATUS="Fermé")])
    fact_contrats_test = spark.createDataFrame([Row(CONTRAT_ID="A", MASTER_CLIENT_ID=1), Row(CONTRAT_ID="B", MASTER_CLIENT_ID=2)])
    result_df = unify_and_map_sinistres(test_ab, test_ap, fact_contrats_test, table_name="gold.fact_sinistre_test")
    result_df.show(truncate=False)
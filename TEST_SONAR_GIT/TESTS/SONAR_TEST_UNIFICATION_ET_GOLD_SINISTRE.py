"""
# Unification Sinistres
fact_sinistre_gold = df_ab_sinistres.select(
    col("AB_CLAIM_ID").cast("string").alias("SINISTRE_ID"),
    col("AB_POLICY_NUMBER").cast("string").alias("CONTRAT_ID"),
    col("AB_MONTANT_ESTIME").alias("MONTANT_INDEMNISE"),
     col("AB_STATUT_SINISTRE").alias("STATUT_SINISTRE")
).union(
    df_ap_sinistres.select(
        col("AP_SINISTRE_NUM").cast("string").alias("SINISTRE_ID"),
        col("AP_CONTRACT_REF").alias("CONTRAT_ID"),
        col("AP_ESTIMATED_AMOUNT").alias("MONTANT_INDEMNISE"),
        col("AP_CLAIM_STATUS").alias("STATUT_SINISTRE")
            )
)


fact_sinistre_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.fact_sinistre")

def unify_sinistres(df_ab_sinistres, df_ap_sinistres, table_name="gold.fact_sinistre"):
    fact_sinistre_gold = df_ab_sinistres.select(
        col("AB_CLAIM_ID").cast("string").alias("SINISTRE_ID"),
        col("AB_POLICY_NUMBER").cast("string").alias("CONTRAT_ID"),
        col("AB_MONTANT_ESTIME").alias("MONTANT_INDEMNISE"),
        col("AB_STATUT_SINISTRE").alias("STATUT_SINISTRE")
    ).union(
        df_ap_sinistres.select(
            col("AP_SINISTRE_NUM").cast("string").alias("SINISTRE_ID"),
            col("AP_CONTRACT_REF").alias("CONTRAT_ID"),
            col("AP_ESTIMATED_AMOUNT").alias("MONTANT_INDEMNISE"),
            col("AP_CLAIM_STATUS").alias("STATUT_SINISTRE")
        )
    )
    fact_sinistre_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
    return fact_sinistre_gold
"""
def test_unify_sinistres():
    from pyspark.sql import Row
    test_ab = spark.createDataFrame([Row(AB_CLAIM_ID="1", AB_POLICY_NUMBER="A", AB_MONTANT_ESTIME=100, AB_STATUT_SINISTRE="Ouvert")])
    test_ap = spark.createDataFrame([Row(AP_SINISTRE_NUM="2", AP_CONTRACT_REF="B", AP_ESTIMATED_AMOUNT=200, AP_CLAIM_STATUS="Fermé")])
    fact_contrats_test = spark.createDataFrame([Row(CONTRAT_ID="A", MASTER_CLIENT_ID=1), Row(CONTRAT_ID="B", MASTER_CLIENT_ID=2)])
    result_df = unify_and_map_sinistres(test_ab, test_ap, fact_contrats_test, table_name="gold.fact_sinistre_test")
    display(result_df)
from pyspark.sql.functions import *


#-----       Ma fonction de unify et map 

def unify_and_map_sinistres(df_ab_sinistres, df_ap_sinistres, fact_contrats_gold, table_name="gold.fact_sinistres"):

# Unification Sinistres
 fact_sinistres_gold = df_ab_sinistres.select(
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

 fact_sinistres_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
# Mapping des sinistres avec la dim_clients pour obtenir MASTER_CLIENT_ID

 fact_sinistres_gold_mapped = fact_sinistres_gold.join(
    fact_contrats_gold.select("CONTRAT_ID", "MASTER_CLIENT_ID"),
    "CONTRAT_ID",
    how="left"
 ).select(
    "SINISTRE_ID",
    "CONTRAT_ID",
    "MASTER_CLIENT_ID",
    "MONTANT_INDEMNISE",
    "STATUT_SINISTRE"
 )

 display(fact_sinistres_gold_mapped)

 fact_sinistres_gold_mapped.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name + "_mapped")

 return fact_sinistres_gold









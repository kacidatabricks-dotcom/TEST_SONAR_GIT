from pyspark.sql.functions import *




def unify_and_map_payments(df_ab_payments, df_ap_payments, fact_contrats_gold, table_name="gold.fact_payments"):

    #(df_ab_sinistres, df_ap_sinistres, fact_contrats_gold, table_name="gold.fact_sinistres"):

    # Unification payments

 fact_payments_gold = df_ab_payments.select(
    col("AB_PAYMENT_ID").cast("string").alias("PAYMENT_ID"),
    col("AB_POLICY_NUMBER").cast("string").alias("CONTRAT_ID"),
    col("AB_DATE_PAYMENT").alias("DATE_PAYMENT"),
    col("AB_MONTANT").alias("MONTANT_INDEMNISE"),
    col("AB_MODE_PAYMENT").alias("MODE_PAYMENT")
  ).union(
    df_ap_payments.select(
    col("AP_PAYMENT_REF").cast("string").alias("PAYMENT_ID"),
    col("AP_CONTRACT_REF").cast("string").alias("CONTRAT_ID"),
    col("AP_PAYMENT_DATETIME").alias("DATE_PAYMENT"),
    col("AP_AMOUNT_PAID").alias("MONTANT_INDEMNISE"),
    col("AP_PAYMENT_CHANNEL").alias("MODE_PAYMENT")
    )
 )

 fact_payments_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)


# Mapping des payments avec la dim_clients pour obtenir MASTER_CLIENT_ID

 fact_payments_gold_mapped = fact_payments_gold.join(
    fact_contrats_gold.select("CONTRAT_ID", "MASTER_CLIENT_ID"),
    "CONTRAT_ID",
    how="left"
 ).select(
    "PAYMENT_ID",
    "CONTRAT_ID",
    "MASTER_CLIENT_ID",
    "DATE_PAYMENT",
    "MONTANT_INDEMNISE",
    "MODE_PAYMENT"
 )

 display(fact_payments_gold_mapped)

 fact_payments_gold_mapped.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name + "_mapped")

 return fact_payments_gold_mapped
    












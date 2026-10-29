from pyspark.sql.functions import col, lit, upper, trim, initcap, lower



#from pyspark.sql.functions import col, lit

def unify_and_map_contrats(df_ab_contrats, df_ap_contrats, dim_clients):
    fact_contrats_gold = None
    try:
        print("Unification contrats en cours...")
        
        # 1. Sélection et Union
        fact_contrats = df_ab_contrats.select(
            col("AB_POLICY_NUMBER").cast("string").alias("CONTRAT_ID"),
            lit("ORACLE").alias("SOURCE_SYSTEM"),
            col("AB_CLIENT_ID").cast("string").alias("ORIGINAL_CLIENT_ID"),
            col("AB_TYPE_ASSURANCE").alias("TYPE_CONTRAT"),
            col("AB_STATUT_CONTRAT").alias("STATUT_CONTRAT"),
            col("AB_PRIME_ANNUELLE").alias("MONTANT_PRIME")
        ).union(
            df_ap_contrats.select(
                col("AP_CONTRACT_REF").cast("string").alias("CONTRAT_ID"),
                lit("SQL").alias("SOURCE_SYSTEM"),
                col("AP_USER_ID").cast("string").alias("ORIGINAL_CLIENT_ID"),
                col("AP_PRODUCT_CODE").alias("TYPE_CONTRAT"),
                col("AP_CONTRACT_STATE").alias("STATUT_CONTRAT"),
                col("AP_MONTHLY_PREMIUM").alias("MONTANT_PRIME")
            )
        )
        
        # 2. Jointure
        fact_contrats_gold = fact_contrats.join(
            dim_clients,
            (fact_contrats.ORIGINAL_CLIENT_ID == dim_clients.ORIGINAL_ID) & 
            (fact_contrats.SOURCE_SYSTEM == dim_clients.SOURCE_SYSTEM)
        ).select("CONTRAT_ID", "MASTER_CLIENT_ID", "TYPE_CONTRAT", "MONTANT_PRIME", "STATUT_CONTRAT")
        
        # ⚡ ACTION FORCÉE : On oblige Spark à analyser les données maintenant
        fact_contrats_gold.first() 
        
        print("Unification contrats réussie.")
        
    except Exception as e:
        print("\n--- ÉCHEC DU PIPELINE ---")
        print("L'unification des contrats n'a pas été réussie.")
        print(f"Détail technique de l'erreur : {e}")
        # On lève à nouveau l'erreur pour empêcher la suite du script de tourner avec un DataFrame vide
        raise e
        
    return fact_contrats_gold


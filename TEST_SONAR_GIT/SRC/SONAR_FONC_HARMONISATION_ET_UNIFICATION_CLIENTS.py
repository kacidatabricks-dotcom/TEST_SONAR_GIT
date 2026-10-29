from pyspark.sql.functions import col, lit, upper, trim, initcap, lower



def harmonize_clients(df_ab_clients, df_ap_clients):
    df_clients=None
    try:
        print ("unification des clients en cours...")
        print(" Colonnes reçues dans le 1er paramètre (df_ab_clients) :", df_ab_clients.columns)
        print("Colonnes reçues dans le 2e paramètre (df_ap_clients) :", df_ap_clients.columns)

        std_df_ab_clients = df_ab_clients.select(
        lit("ORACLE").alias("SOURCE_SYSTEM"),
        col("AB_CLIENT_ID").cast("string").alias("ORIGINAL_ID"),
        upper(trim(col("AB_NOM"))).alias("NOM"),
        initcap(trim(col("AB_PRENOM"))).alias("PRENOM"),
        col("AB_DATE_NAISSANCE").alias("DATE_NAISSANCE"),
        lower(trim(col("AB_EMAIL"))).alias("EMAIL"),
        col("AB_TELEPHONE").alias("TELEPHONE"),
        col("AB_ADRESSE").alias("ADRESSE"),
        col("AB_CODE_POSTAL").alias("CODE_POSTAL"),
        col("AB_NUM_FISCAL").alias("NUMERO_FISCAL"),
        col("AB_DATE_CREATION").alias("DATE_CREATION"),
        col("AB_STATUT_CLIENT").alias("STATUT_CLIENT"),
        col("INGESTION_TIMESTAMP").alias("INGESTION_TIMESTAMP"),
        col("SOURCE_FILE").alias("SOURCE_FILE")
    )
        std_df_ap_clients = df_ap_clients.select(
        lit("SQL").alias("SOURCE_SYSTEM"),
        col("AP_USER_ID").cast("string").alias("ORIGINAL_ID"),
        upper(trim(col("AP_NOM"))).alias("NOM"),
        initcap(trim(col("AP_PRENOM"))).alias("PRENOM"),
        col("AP_BIRTH_DATE").alias("DATE_NAISSANCE"),
        lower(trim(col("AP_MAIL_ADDRESS"))).alias("EMAIL"),
        col("AP_PHONE_NUMBER").alias("TELEPHONE"),
        col("AP_STREET_ADDRESS").alias("ADRESSE"),
        col("AP_ZIP_CODE").alias("CODE_POSTAL"),
        col("AP_NUM_FISCAL").alias("NUMERO_FISCAL"),
        col("AP_DATE_CREATION").alias("DATE_CREATION"),
        col("AP_CUSTOMER_STATUS").alias("STATUT_CLIENT"),
        col("INGESTION_TIMESTAMP").alias("INGESTION_TIMESTAMP"),
        col("SOURCE_FILE").alias("SOURCE_FILE")
    )
        df_clients = std_df_ab_clients.union(std_df_ap_clients)
        df_clients.createOrReplaceTempView("raw_unified_clients")
        df_clients.first()
        print("unification des clients réussie")

        return df_clients
    

    except Exception as e:
        print("\n--- ÉCHEC DU PIPELINE ---")
        print("L'unification des clients  a échoué.")
        print(f"Détail technique de l'erreur : {e}")
        # On lève à nouveau l'erreur pour empêcher la suite du script de tourner avec dtaframes vides
        raise e


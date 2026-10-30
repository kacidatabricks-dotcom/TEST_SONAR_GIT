# Ajout des données pour avoir la trace


from pyspark.sql import functions as F

# 1. Définition des fonctions
def ajoutmétadonnées(df, source_system, source_file, client_id_col):
    df.first()
    try:
     print("ajout de métadonnées en cours...")   

     print("ajout des métadonnées")
     return df \
        .withColumn("SOURCE_SYSTEM", F.lit(source_system)) \
        .withColumn("ORIGINAL_ID", F.col(client_id_col).cast("string")) \
        .withColumn("SOURCE_FILE", F.lit(source_file)) \
        .withColumn("INGESTION_TIMESTAMP", F.current_timestamp())

    except Exception as e:
        print("\n--- ÉCHEC DU PIPELINE ---")
        print("L'ajout des métédonnées a échoué.")
        print(f"Détail technique de l'erreur : {e}")
        # On lève à nouveau l'erreur pour empêcher la suite du script de tourner avec dtaframes vides
        raise e




# 2. Exécution de la chaîne (Exemple avec un DataFrame initialisé)
# oracle_clients = spark.read...

'''
# ORACLE
df_ab_clients =ajoutmétadonnées (df_ab_clients,"Oracle","Oracle_clients.csv","AB_CLIENT_ID")

#SQL Server
df_ap_clients =ajoutmétadonnées (df_ap_clients,"SQL","SQL_clients.csv","AP_USER_ID")


display(df_ab_clients)
display(df_ap_clients)
'''


# Ajout des données pour avoir la trace   


from pyspark.sql import functions as F
from pyspark.sql.functions import expr

# 1. Définition des fonctions
def ajoutmétadonnées(df, source_system, source_file, client_id_col):
    df.first()
    try:
     print("ajout de métadonnées en cours...", source_system)  

     print("ajout des métadonnées")
     return df \
        .withColumn("SOURCE_SYSTEM", F.lit(source_system)) \
        .withColumn("ORIGINAL_ID", F.col(client_id_col)) \
        .withColumn("SOURCE_FILE", F.lit(source_file)) \
        .withColumn("INGESTION_TIMESTAMP", F.current_timestamp())

    except Exception as e:
        print("\n--- ÉCHEC DU PIPELINE ---")
        print("L'ajout des métédonnées a échoué.")
        print(f"Détail technique de l'erreur : {e}")
        # On lève à nouveau l'erreur pour empêcher la suite du script de tourner avec dtaframes vides
        raise e






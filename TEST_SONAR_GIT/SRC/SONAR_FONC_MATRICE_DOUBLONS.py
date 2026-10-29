"""
# Étape 3 : Matrice de Doublons et Création de la Dimension Client

# Algorithme d'identification des doublons (Matching exact sur Nom, Prénom, Date de Naissance ou Téléphone)
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, upper, initcap

# Génération d'une clé de rapprochement
matched_clients = df_clients.withColumn(
    "MATCH_KEY", 
    concat_ws("_", col("NOM"), col("PRENOM"), col("DATE_NAISSANCE"))
)

# Attribution d'un ID Master Unique pour les doublons
windowSpec = Window.orderBy("MATCH_KEY")
dim_client = matched_clients.withColumn("MASTER_CLIENT_ID", dense_rank().over(windowSpec))


# Affichage de la Matrice de Doublons
print("--- MATRICE DE DOUBLONS IDENTIFIÉS ---")
doublons_df = dim_client.groupBy("MASTER_CLIENT_ID") \
    .agg(count("ORIGINAL_ID").alias("NB_OCCURENCES")) \
    .filter(col("NB_OCCURENCES") > 1)

display(dim_client.join(doublons_df, "MASTER_CLIENT_ID"))
"""



def generer_Matrice_Doublons(df_clients):
    from pyspark.sql.window import Window
    from pyspark.sql.functions import dense_rank, upper, initcap, concat_ws, col, count
    
    matched_clients = None 
    dim_clients = None
    doublons_df = None
    result_df= None
    try:
     print("Rechreche doublons  en cours...")


     matched_clients = df_clients.withColumn(
        "MATCH_KEY", 
        concat_ws("_", col("NOM"), col("PRENOM"), col("DATE_NAISSANCE"))
    )
# Attribution d'un ID Master Unique pour les doublons
     windowSpec = Window.orderBy("MATCH_KEY")
     #dim_clients = matched_clients.withColumn("MASTER_CLIENT_ID", dense_rank().over(windowSpec))
     dim_clients = matched_clients.withColumn("MASTER_CLIENT_ID", dense_rank().over(windowSpec)).localCheckpoint()

# Affichage de la matrice de doublons

     doublons_df = dim_clients.groupBy("MASTER_CLIENT_ID") \
        .agg(count("ORIGINAL_ID").alias("NB_OCCURENCES")) \
        .filter(col("NB_OCCURENCES") > 1)

     result_df = dim_clients.join(doublons_df, "MASTER_CLIENT_ID")
     matched_clients.first() 
     dim_clients.first()
     #doublons_df.first()
     result_df.first()    
     print("Création maatrice doublons  réussie.")

     return   matched_clients, dim_clients, doublons_df,result_df
    except Exception as e:
        print("\n--- ÉCHEC DU PIPELINE ---")
        print("La création de la matrice des doublons a échoué.")
        print(f"Détail technique de l'erreur : {e}")
        # On lève à nouveau l'erreur pour empêcher la suite du script de tourner avec dtaframes vides
        raise e

 



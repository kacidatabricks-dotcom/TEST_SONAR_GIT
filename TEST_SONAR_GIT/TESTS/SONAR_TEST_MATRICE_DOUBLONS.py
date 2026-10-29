# Étape 3 : Matrice de Doublons et Création de la Dimension Client

# Algorithme d'identification des doublons (Matching exact sur Nom, Prénom, Date de Naissance ou Téléphone)
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, upper, initcap
"""
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




def generer_matrice_doublons(df_clients):
    from pyspark.sql.window import Window
    from pyspark.sql.functions import dense_rank, upper, initcap, concat_ws, col, count

    matched_clients = df_clients.withColumn(
        "MATCH_KEY", 
        concat_ws("_", col("NOM"), col("PRENOM"), col("DATE_NAISSANCE"))
    )

    windowSpec = Window.orderBy("MATCH_KEY")
    dim_client = matched_clients.withColumn("MASTER_CLIENT_ID", dense_rank().over(windowSpec))

    doublons_df = dim_client.groupBy("MASTER_CLIENT_ID") \
        .agg(count("ORIGINAL_ID").alias("NB_OCCURENCES")) \
        .filter(col("NB_OCCURENCES") > 1)

    result_df = dim_client.join(doublons_df, "MASTER_CLIENT_ID")
    return result_df
"""
def test_generer_matrice_doublons():
    from pyspark.sql import Row
    test_data = [
        Row(NOM="Dupont", PRENOM="Jean", DATE_NAISSANCE="1980-01-01", ORIGINAL_ID=1),
        Row(NOM="Dupont", PRENOM="Jean", DATE_NAISSANCE="1980-01-01", ORIGINAL_ID=2),
        Row(NOM="Martin", PRENOM="Pierre", DATE_NAISSANCE="1990-02-02", ORIGINAL_ID=3)
    ]
    df_test = spark.createDataFrame(test_data)
    df_result = generer_matrice_doublons(df_test)
    display(df_result)

# Exemple d'utilisation:
# result = generer_matrice_doublons(df_clients)
# display(result)

# Pour tester :
# test_generer_matrice_doublons()
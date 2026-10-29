# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType
from pyspark.sql.functions import col, concat, lit, when, current_timestamp, to_date, trim, upper, split, row_number, lower
from pyspark.sql.window import Window
import re
from pyspark.sql import functions as F
# Initialisation de la session Spark
spark = SparkSession.builder.appName("Client_Merge").getOrCreate()

# Définition du schéma cible
target_schema = StructType([
    StructField("client_id", IntegerType(), False),
    StructField("source_system", StringType(), True),
    StructField("source_client_id", StringType(), True),
    StructField("nom", StringType(), True),
    StructField("prenom", StringType(), True),
    StructField("nom_complet", StringType(), True),
    StructField("date_naissance", DateType(), True),
    StructField("email", StringType(), True),
    StructField("telephone", StringType(), True),
    StructField("adresse", StringType(), True),
    StructField("code_postal", StringType(), True),
    StructField("numero_fiscal", StringType(), True),
    StructField("date_creation", TimestampType(), True),
    StructField("statut_client", StringType(), True),
    StructField("score_fidelite", IntegerType(), True),
    StructField("date_integration", TimestampType(), True)
])

# Fonction pour traiter le système AB (source 1)
def process_ab_source(ab_client_df):
    """
    Traite les données de la source AB (AB_CLIENT)
    """
    # Sélection des colonnes et mapping vers le schéma cible
    ab_processed = ab_client_df.select(
        col("AB_CLIENT_ID").cast(StringType()).alias("source_client_id"),
        lit("AB").alias("source_system"),
        trim(upper(col("AB_NOM"))).alias("nom"),
        trim(upper(col("AB_PRENOM"))).alias("prenom"),
        col("AB_DATE_NAISSANCE").cast(DateType()).alias("date_naissance"),
        trim(lower(col("AB_EMAIL"))).alias("email"),
        col("AB_TELEPHONE").alias("telephone"),
        col("AB_ADRESSE").alias("adresse"),
        col("AB_CODE_POSTAL").alias("code_postal"),
        col("AB_NUM_FISCAL").alias("numero_fiscal"),
        col("AB_DATE_CREATION").cast(TimestampType()).alias("date_creation"),
        col("AB_STATUT_CLIENT").alias("statut_client")
    )
    
    # Construction du nom complet
    ab_processed = ab_processed.withColumn(
        "nom_complet",
        when(
            col("nom").isNotNull() & col("prenom").isNotNull(), 
            concat(col("prenom"), lit(" "), col("nom"))
        ).when(col("nom").isNotNull(), col("nom"))
        .otherwise(col("prenom"))
    )
    
    # Ajout du score de fidélité (défaut 0 pour AB)
    ab_processed = ab_processed.withColumn("score_fidelite", lit(0).cast(IntegerType()))
    
    # Ajout de la date d'intégration
    ab_processed = ab_processed.withColumn("date_integration", current_timestamp())
    
    # Génération de l'ID client pour AB
    ab_processed = ab_processed.withColumn(
        "client_id",
        when(
            col("source_client_id").isNotNull(),
            (lit(1000000) + col("source_client_id").cast(IntegerType())).cast(IntegerType())
        ).otherwise(lit(0))
    )
    # ajout pour formater les téléphones
    Format_téléphone(ab_client_df)

    return ab_processed

# Fonction pour traiter le système AP (source 2)
def process_ap_source(ap_users_df):
    """
    Traite les données de la source AP (AP_USERS)
    """
    # Sélection des colonnes et mapping vers le schéma cible
    ap_processed = ap_users_df.select(
        col("AP_USER_ID").cast(StringType()).alias("source_client_id"),
        lit("AP").alias("source_system"),
        trim(upper(col("AP_FULL_NAME"))).alias("nom_complet_original"),
        # Conversion du format de date YYYY-MM-DD vers DATE
        to_date(col("AP_BIRTH_DATE"), "yyyy-MM-dd").alias("date_naissance"),
        trim(lower(col("AP_MAIL_ADDRESS"))).alias("email"),
        col("AP_PHONE_NUMBER").alias("telephone"),
        col("AP_STREET_ADDRESS").alias("adresse"),
        col("AP_ZIP_CODE").alias("code_postal"),
        # Pas de numéro fiscal dans AP
        lit(None).cast(StringType()).alias("numero_fiscal"),
        # Conversion DATETIME vers TIMESTAMP
        col("AP_CREATED_AT").cast(TimestampType()).alias("date_creation"),
        col("AP_CUSTOMER_STATUS").alias("statut_client"),
        col("AP_LOYALTY_SCORE").cast(IntegerType()).alias("score_fidelite")
    )
    
    # Extraction du nom et prénom à partir du nom complet
    ap_processed = ap_processed.withColumn(
        "nom_complet",
        col("nom_complet_original")
    )
    
    # Tentative d'extraction du nom et prénom (séparés par espace)
    ap_processed = ap_processed.withColumn(
        "prenom",
        when(
            col("nom_complet").isNotNull() & (col("nom_complet").rlike(" ")),
            trim(split(col("nom_complet"), " ").getItem(0))
        ).when(
            col("nom_complet").isNotNull() & ~col("nom_complet").rlike(" "),
            col("nom_complet")
        ).otherwise(lit(None))
    ).withColumn(
        "nom",
        when(
            col("nom_complet").isNotNull() & (col("nom_complet").rlike(" ")),
            trim(split(col("nom_complet"), " ").getItem(1))
        ).otherwise(lit(None))
    )
    
    # Ajout de la date d'intégration
    ap_processed = ap_processed.withColumn("date_integration", current_timestamp())
    
    # Génération de l'ID client pour AP
    ap_processed = ap_processed.withColumn(
        "client_id",
        when(
            col("source_client_id").isNotNull(),
            (lit(2000000) + col("source_client_id").cast(IntegerType())).cast(IntegerType())
        ).otherwise(lit(0))
    )
    
    # Suppression des colonnes temporaires
    ap_final = ap_processed.drop("nom_complet_original")
    
    return ap_final

# Fonction pour gérer les doublons entre les sources
def resolve_duplicates(df):
    """
    Résolution des doublons: priorité à la source AB sur la source AP
    """
    # Définir une fenêtre pour identifier les doublons par source_client_id
    window_spec = Window.partitionBy("source_client_id").orderBy(
        when(col("source_system") == "AB", 1).otherwise(2),
        col("date_creation").desc_nulls_last()
    )
    
    
    # Ajouter un numéro de ligne et filtrer pour garder le premier
    df_resolved = df.withColumn("row_num", row_number().over(window_spec)) \
                    .filter(col("row_num") == 1) \
                    .drop("row_num")
    
    # je l'ai rajouté
    print("\nAperçu des doublons:")
    df_resolved.show()
    return df_resolved

# Fonction principale de fusion
def merge_clients(spark, ab_client_df, ap_users_df):
    """
    Fonction principale qui fusionne les clients des deux sources
    """
    print("Début du traitement de fusion des clients...")
    
    # Traitement des deux sources
    print("Traitement de la source AB...")
    ab_data = process_ab_source(ab_client_df)
    print(f"Nombre de clients AB: {ab_data.count()}")
    
    print("Traitement de la source AP...")
    ap_data = process_ap_source(ap_users_df)
    print(f"Nombre de clients AP: {ap_data.count()}")
    
    # Union des deux dataframes
    print("Union des données...")
    combined_df = ab_data.unionByName(ap_data, allowMissingColumns=True)
    
    print("Vérification du schéma...")
    # S'assurer que toutes les colonnes du schéma cible sont présentes
    for field in target_schema.fields:
        if field.name not in combined_df.columns:
            combined_df = combined_df.withColumn(field.name, lit(None).cast(field.dataType))
    
    print("Résolution des doublons...")
    final_df = resolve_duplicates(combined_df)
    
    # Réorganisation des colonnes selon le schéma cible
    final_ordered = final_df.select(target_schema.fieldNames())
    
    print(f"Traitement terminé. {final_ordered.count()} clients uniques dans le résultat.")
    
    return final_ordered
# Formattage du numéro de téléphone

def Format_téléphone(df):
# 1. Nettoyer pour garder uniquement les chiffres
# Supprime tout ce qui n'est pas un chiffre (espaces, points, tirets, +)
 df_clean = df.withColumn("digits", F.regexp_replace(F.col("AB_TELEPHONE"), r"\D", ""))

# 2. Gérer le 0 initial français et formater par blocs de 2 chiffres
# Si le numéro commence par 33, on le garde ; si par 0, on le remplace par 33
 df_normalized = df_clean.withColumn(
    "intl_digits",
    F.when(F.col("digits").startswith( "0"), F.concat(F.lit("33"), F.substring(F.col("digits"), 2, 20)))
     .otherwise(F.col("digits"))
)

# 3. Appliquer le format +33 XX XX XX XX...
 df_formatted = df_normalized.withColumn(
    "AB_TELEPHONE_format",
    F.regexp_replace(F.col("intl_digits"), r"(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})", "+$1 $2 $3 $4 $5")
)

 df_formatted.select("AB_TELEPHONE", "AB_TELEPHONE_format").show(truncate=False)
 # je l ai rajouté
 df= df.withColumn("AB_TELEPHONE",df_formatted["AB_TELEPHONE_format"])
  #return df
 #%run ./FormattélephoneAssurPlus.py







# Exemple d'utilisation dans Databricks
if __name__ == "__main__":
    try:
        # Pour tester avec des données d'exemple si les tables n'existent pas
        # Création de données d'exemple pour AB
        ab_data = [
            (1, "DUPONT", "Jean", "1990-05-15", "jean.dupont@email.com", "0123456789", 
             "15 rue de Paris", "75001", "FR123456789", "2020-01-01 10:00:00", "ACTIF"),
            (2, "MARTIN", "Marie", "1985-08-22", "marie.martin@email.com", "0987654321",
             "25 avenue des Champs", "75008", "FR987654321", "2020-02-15 14:30:00", "INACTIF")
        ]
        
        ab_schema = StructType([
            StructField("AB_CLIENT_ID", IntegerType(), True),
            StructField("AB_NOM", StringType(), True),
            StructField("AB_PRENOM", StringType(), True),
            StructField("AB_DATE_NAISSANCE", StringType(), True),
            StructField("AB_EMAIL", StringType(), True),
            StructField("AB_TELEPHONE", StringType(), True),
            StructField("AB_ADRESSE", StringType(), True),
            StructField("AB_CODE_POSTAL", StringType(), True),
            StructField("AB_NUM_FISCAL", StringType(), True),
            StructField("AB_DATE_CREATION", StringType(), True),
            StructField("AB_STATUT_CLIENT", StringType(), True)
        ])
        
        #ab_client_df = spark.createDataFrame(ab_data, schema=ab_schema)
        
        # Création de données d'exemple pour AP
        ap_data = [
            (1001, "DURAND Pierre", "1992-03-10", "pierre.durand@email.com", "0654321098",
             "5 rue de la République", "69002", "2021-05-20 09:00:00", "PREMIUM", 85),
            (1002, "LEROY Sophie", "1988-11-05", "sophie.leroy@email.com", "0678901234" ,
            "35 boulevard Victor Hugo", "13001", "2021-06-10 11:30:00", "STANDARD", 60)
            
        ]
        
        ap_schema = StructType([
            StructField("AP_USER_ID", IntegerType(), True),
            StructField("AP_FULL_NAME", StringType(), True),
            StructField("AP_BIRTH_DATE", StringType(), True),
            StructField("AP_MAIL_ADDRESS", StringType(), True),
            StructField("AP_PHONE_NUMBER", StringType(), True),
            StructField("AP_STREET_ADDRESS", StringType(), True),
            StructField("AP_ZIP_CODE", StringType(), True),
            StructField("AP_CREATED_AT", StringType(), True),
            StructField("AP_CUSTOMER_STATUS", StringType(), True),
            StructField("AP_LOYALTY_SCORE", IntegerType(), True)
        ])
        
       # ap_users_df = spark.createDataFrame(ap_data, schema=ap_schema)

        path_ab_clients = "/Workspace/Users/kacidatabricks@gmail.com/Projet Assurance/abassurance_databricks_bloc3_kit/DATA/SAMPLE/ABASSURANCE/clients.csv"
        path_ap_users = "/Workspace/Users/kacidatabricks@gmail.com/Projet Assurance/abassurance_databricks_bloc3_kit/DATA/SAMPLE/APASSURANCE/users.csv"
        ab_client_df = spark.read.option("header", True).option("inferSchema", True).csv(path_ab_clients)
        ap_users_df = spark.read.option("header", True).option("inferSchema", True).csv(path_ap_users)

        # Formattage des numéros de téléphone
        # ajout pour formater les téléphones
        Format_téléphone(ab_client_df)
        print('premeir display')
        display(ab_client_df)


        # Fusion des données
        print("\n" + "="*50)
        print("FUSION DES DONNÉES CLIENTS")
        print("="*50)
        
        result_df = merge_clients(spark, ab_client_df, ap_users_df)
        
        # Affichage des résultats
        print("\nAperçu des résultats:")
        result_df.show(10, truncate=False)
        
        print(f"\nNombre total de clients fusionnés: {result_df.count()}")
        print(f"Nombre de clients AB: {result_df.filter(col('source_system') == 'AB').count()}")
        print(f"Nombre de clients AP: {result_df.filter(col('source_system') == 'AP').count()}")
        
        # Vérification du schéma
        print("\nSchéma du dataframe final:")
        result_df.printSchema()
        
        # Sauvegarde des résultats (optionnel)
        # result_df.write.mode("overwrite").saveAsTable("default.clients_merged")
        # result_df.write.mode("overwrite").parquet("/mnt/data/clients_merged")
        
    except Exception as e:
        print(f"\n❌ Erreur lors de l'exécution: {e}")
        import traceback
        traceback.print_exc()
        raise
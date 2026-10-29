# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, time
from pyspark.sql.functions import col, concat, lit, when, current_timestamp, to_date, coalesce, trim, upper, split, row_number
import re
from pyspark.sql.window import Window


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

def clean_string(value):
    """Fonction de nettoyage des chaînes de caractères"""
    if value is None:
        return None
    return trim(upper(str(value)))

# Fonction pour traiter le système AB (source 1)
def process_ab_source(spark, ab_client_df, ab_contrat_df=None):
    """
    Traite les données de la source AB (AB_CLIENT)
    """
    # Sélection des colonnes et mapping vers le schéma cible
    ab_processed = ab_client_df.select(
        col("AB_CLIENT_ID").cast(IntegerType()).alias("source_client_id"),
        lit("AB").alias("source_system"),
        col("AB_NOM").alias("nom_original"),
        col("AB_PRENOM").alias("prenom_original"),
        col("AB_DATE_NAISSANCE").cast(DateType()).alias("date_naissance"),
        col("AB_EMAIL").alias("email"),
        col("AB_TELEPHONE").alias("telephone"),
        col("AB_ADRESSE").alias("adresse"),
        col("AB_CODE_POSTAL").alias("code_postal"),
        col("AB_NUM_FISCAL").alias("numero_fiscal"),
        col("AB_DATE_CREATION").cast(TimestampType()).alias("date_creation"),
        col("AB_STATUT_CLIENT").alias("statut_client")
    )
    
    # Nettoyage et transformation des champs
    ab_processed = ab_processed.withColumn(
        "nom",
        when(col("nom_original").isNotNull(), trim(upper(col("nom_original"))))
        .otherwise(lit(None))
    ).withColumn(
        "prenom",
        when(col("prenom_original").isNotNull(), trim(upper(col("prenom_original"))))
        .otherwise(lit(None))
    ).withColumn(
        "nom_complet",
        when(col("nom").isNotNull() & col("prenom").isNotNull(), 
             concat(col("nom"), lit(" "), col("prenom")))
        .when(col("nom").isNotNull(), col("nom"))
        .otherwise(col("prenom"))
    )
    
    # Ajout du score de fidélité (défaut 0 pour AB)
    ab_processed = ab_processed.withColumn("score_fidelite", lit(0).cast(IntegerType()))
    
    # Ajout de la date d'intégration
    ab_processed = ab_processed.withColumn("date_integration", current_timestamp())
    
    # Suppression des colonnes temporaires
    ab_final = ab_processed.drop("nom_original", "prenom_original")
    
    return ab_final

# Fonction pour traiter le système AP (source 2)
def process_ap_source(spark, ap_users_df, ap_contrats_df=None):
    """
    Traite les données de la source AP (AP_USERS)
    """
    # Sélection des colonnes et mapping vers le schéma cible
    ap_processed = ap_users_df.select(
        col("AP_USER_ID").cast(IntegerType()).alias("source_client_id"),
        lit("AP").alias("source_system"),
        col("AP_FULL_NAME").alias("nom_complet_original"),
        # Conversion du format de date YYYY-MM-DD vers DATE
        to_date(col("AP_BIRTH_DATE"), "yyyy-MM-dd").alias("date_naissance"),
        col("AP_MAIL_ADDRESS").alias("email"),
        col("AP_PHONE_NUMBER").alias("telephone"),
        col("AP_STREET_ADDRESS").alias("adresse"),
        col("AP_ZIP_CODE").alias("code_postal"),
        # Pas de numéro fiscal dans AP
        lit(None).alias("numero_fiscal"),
        # Conversion DATETIME vers TIMESTAMP
        col("AP_CREATED_AT").cast(TimestampType()).alias("date_creation"),
        col("AP_CUSTOMER_STATUS").alias("statut_client"),
        col("AP_LOYALTY_SCORE").cast(IntegerType()).alias("score_fidelite")
    )
    
    # Extraction du nom et prénom à partir du nom complet
    ap_processed = ap_processed.withColumn(
        "nom_complet",
        when(col("nom_complet_original").isNotNull(), trim(upper(col("nom_complet_original"))))
        .otherwise(lit(None))
    )
    
    # Tentative d'extraction du nom et prénom (séparés par espace)
    ap_processed = ap_processed.withColumn(
        "nom",
        when(
            col("nom_complet").isNotNull() & (col("nom_complet").rlike(" ")),
            trim(split(col("nom_complet"), " ").getItem(1))  # Dernier mot = nom
        )
        .when(col("nom_complet").isNotNull() & ~col("nom_complet").rlike(" "),
              col("nom_complet"))  # Cas d'un seul mot
        .otherwise(lit(None))
    ).withColumn(
        "prenom",
        when(
            col("nom_complet").isNotNull() & (col("nom_complet").rlike(" ")),
            trim(split(col("nom_complet"), " ").getItem(0))  # Premier mot = prénom
        )
        .otherwise(lit(None))
    )
    
    # Ajout de la date d'intégration
    ap_processed = ap_processed.withColumn("date_integration", current_timestamp())
    
    # Suppression des colonnes temporaires
    ap_final = ap_processed.drop("nom_complet_original")
    
    return ap_final

# Fonction pour gérer les doublons entre les sources
def resolve_duplicates(df):
    """
    Résolution des doublons: priorité à la source AB sur la source AP
    """
    # Attribution d'un poids plus élevé à AB pour garder la donnée la plus récente
    duplicate_config = {
        "source_system_order": {"AB": 1, "AP": 2},
        "fields_to_keep": ["nom", "prenom", "nom_complet", "date_naissance", "email", 
                          "telephone", "adresse", "code_postal", "numero_fiscal", 
                          "date_creation", "statut_client", "score_fidelite"]
    }
    
    # Grouper par source_client_id pour détecter les doublons
    window_spec = Window.partitionBy("source_client_id").orderBy(
        when(col("source_system") == "AB", 1).otherwise(2),
        col("date_creation").desc()
    )
    
    # Prendre le premier enregistrement par client selon l'ordre défini
    df_resolved = df.withColumn("row_num", row_number().over(window_spec)) \
                    .filter(col("row_num") == 1) \
                    .drop("row_num")
    
    return df_resolved

# Fonction principale de fusion
def merge_clients(spark, ab_client_df, ap_users_df):
    """
    Fonction principale qui fusionne les clients des deux sources
    """
    print("Début du traitement de fusion des clients...")
    
    # Traitement des deux sources
    print("Traitement de la source AB...")
    ab_data = process_ab_source(spark, ab_client_df)
    
    print("Traitement de la source AP...")
    ap_data = process_ap_source(spark, ap_users_df)
    
    # Union des deux dataframes
    print("Union des données...")
    combined_df = ab_data.unionByName(ap_data, allowMissingColumns=True)
    
    # Remplir les valeurs manquantes pour les colonnes non-nullables
    print("Remplissage des valeurs manquantes...")
    combined_df = combined_df.fillna({
        "client_id": 0,  # Valeur par défaut pour les ID
        "source_system": "UNKNOWN",
        "source_client_id": "0",
        "nom": "N/A",
        "prenom": "N/A",
        "nom_complet": "N/A",
        "score_fidelite": 0
    })
    
    # Résolution des doublons
    print("Résolution des doublons...")
    final_df = resolve_duplicates(combined_df)
# je l 'ai rajouté
    display(final_df)
    time.sleep(20)



    # Génération d'un client_id unique pour chaque enregistrement
    print("Génération des IDs uniques...")
    # Vérifier s'il y a déjà des client_id
    
    if not final_df.filter(col("source_client_id").isNull()).count() > 0:
        # Créer un ID basé sur le système source et l'ID source
        final_df = final_df.withColumn(
            "client_id",
            when(col("source_system") == "AB", 
                 concat(lit(1000000), col("source_client_id")))
            .when(col("source_system") == "AP",
                  concat(lit(2000000), col("source_client_id")))
            .otherwise(concat(lit(9000000), col("source_client_id")))
        )
    
    # Vérification des types et du schéma final
    for field in target_schema.fields:
        if field.name not in final_df.columns:
            final_df = final_df.withColumn(field.name, lit(None).cast(field.dataType))
    
    # Réorganisation des colonnes selon le schéma cible
    final_ordered = final_df.select([field.name for field in target_schema.fields])
    
    print(f"Traitement terminé. {final_ordered.count()} clients uniques dans le résultat.")
    
    return final_ordered

# Exemple d'utilisation dans Databricks
if __name__ == "__main__":
    # Lecture des données (exemple avec les données d'exemple)
    try:
        # Définition des chemins des tables dans Databricks
        #AB_CLIENT_TABLE = "default.ab_client"
        #AP_USERS_TABLE = "default.ap_users"
        
        # Lecture des données
        # ab_client_df = spark.table(AB_CLIENT_TABLE)
        #ap_users_df = spark.table(AP_USERS_TABLE)
         path_ab_clients = "/Workspace/Users/kacidatabricks@gmail.com/Projet Assurance/abassurance_databricks_bloc3_kit/DATA/SAMPLE/ABASSURANCE/clients.csv"
         path_ap_users = "/Workspace/Users/kacidatabricks@gmail.com/Projet Assurance/abassurance_databricks_bloc3_kit/DATA/SAMPLE/APASSURANCE/users.csv"
         ab_client_df = spark.read.option("header", True).option("inferSchema", True).csv(path_ab_clients)
         ap_users_df = spark.read.option("header", True).option("inferSchema", True).csv(path_ap_users)

        # Fusion des données
         result_df = merge_clients(spark, ab_client_df, ap_users_df)
        
        # Affichage des résultats
         print("Aperçu des résultats:")
         result_df.show(10, truncate=False)
        
        # Statistiques
         print(f"Nombre total de clients fusionnés: {result_df.count()}")
         print(f"Nombre de clients AB: {result_df.filter(col('source_system') == 'AB').count()}")
         print(f"Nombre de clients AP: {result_df.filter(col('source_system') == 'AP').count()}")
        
        # Sauvegarde des résultats (optionnel)
        # result_df.write.mode("overwrite").saveAsTable("default.clients_merged")
        
    except Exception as e:
        print(f"Erreur lors de l'exécution: {e}")
        raise

    """
    Nettoyage des données : Normalisation des formats (uppercase, trim)

Mapping des colonnes : Adaptation des schémas des deux sources vers le schéma cible

Conversion de types : Gestion des différents formats de date et types

Gestion des noms complets : Extraction du nom/prénom depuis AP_USERS

Résolution de doublons : Priorité donnée à la source AB sur AP

Génération d'IDs uniques : Création d'identifiants distincts pour chaque système source

Vérification du schéma : Validation et complétion du schéma final

Le programme peut être exécuté directement dans un notebook Databricks.



    """
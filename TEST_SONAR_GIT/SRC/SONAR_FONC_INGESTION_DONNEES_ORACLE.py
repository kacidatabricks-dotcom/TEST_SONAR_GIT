

from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap, col, lower, upper, trim, concat_ws, levenshtein, when, lit, count, sum as _sum, avg, round, to_date
from pyspark.sql.types import *
#from pyspark.sql.functions import *
import pandas as pd
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, DoubleType
<<<<<<< HEAD
=======
from pathlib import Path
# Initialisation Spark
# spark = SparkSession.builder.appName("AssuranceData").getOrCreate()  supprimée parès le conseil de chat
>>>>>>> origin/Develop


def create_ab_dataframes(spark,data_dir=None):
    if data_dir is None:
         data_dir = (
            Path(__file__).resolve().parents[1]
            / "DATA"
            / "SAMPLE"
            / "ABASSURANCE"
        ).as_uri()

    data_dir = data_dir.rstrip("/")
    df_ab_clients= None
    df_ab_contrats = None
    df_ab_sinistres = None
    df_ab_payments = None
    try:
        print("Création des dataframes AbAssurances en cours...")
        # Constantes de valeurs pour types et modes
        TYPE_SANTE = "Santé"
        TYPE_HABITATION = "Habitation"
        TYPE_AUTO = "Auto"
        TYPE_PREVOYANCE = "Prévoyance"

        MODE_CARTE_BANCAIRE = "Carte bancaire"
        MODE_VIREMENT = "Virement"
        MODE_PRELEVEMENT = "Prélèvement"
        MODE_CHEQUE = "Chèque"   
        # Définition des schemas
        ab_clients_schema = StructType([
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
        
        ab_contrats_schema = StructType([
            StructField("AB_POLICY_NUMBER", StringType(), True),
            StructField("AB_CLIENT_ID", IntegerType(), True),
            StructField("AB_TYPE_ASSURANCE", StringType(), True),
            StructField("AB_DATE_DEBUT", StringType(), True),
            StructField("AB_DATE_FIN", StringType(), True),
            StructField("AB_PRIME_ANNUELLE", DoubleType(), True),
            StructField("AB_STATUT_CONTRAT", StringType(), True),
            StructField("AB_AGENCE_ID", IntegerType(), True)
        ])
        
        ab_sinistres_schema = StructType([
            StructField("AB_CLAIM_ID", IntegerType(), True),
            StructField("AB_POLICY_NUMBER", StringType(), True),
            StructField("AB_DATE_SINISTRE", StringType(), True),
            StructField("AB_MONTANT_ESTIME", DoubleType(), True),
            StructField("AB_STATUT_SINISTRE", StringType(), True),
            StructField("AB_DESCRIPTION", StringType(), True)
        ])
        
        ab_payments_schema = StructType([
            StructField("AB_PAYMENT_ID", IntegerType(), True),
            StructField("AB_POLICY_NUMBER", StringType(), True),
            StructField("AB_DATE_PAYMENT", StringType(), True),
            StructField("AB_MONTANT", DoubleType(), True),
            StructField("AB_MODE_PAYMENT", StringType(), True)
        ])
        
  
        # Création des dataframes

        # Schéma explicite (défini plus haut) : avec inferSchema, AB_TELEPHONE est converti
        # en entier (le 0 initial est perdu) et le contrôle téléphonique en Silver rejette
        # quasiment tous les clients -> MASTER_CLIENT_ID null dans les tables Gold.
        df_ab_clients = (
        spark.read.format("csv")
        .option("header", True)
        .schema(ab_clients_schema)
<<<<<<< HEAD
         .load("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/TEST_SONAR_GIT/DATA/SAMPLE/ABASSURANCE/clients.csv"))
=======
        #.load("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/TEST_SONAR_GIT/DATA/SAMPLE/ABASSURANCE/clients.csv")) # NOSONAR
        .load(f"{data_dir}/clients.csv"))
>>>>>>> origin/Develop

        df_ab_contrats = (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", "true")
<<<<<<< HEAD
        .load("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/TEST_SONAR_GIT/DATA/SAMPLE/ABASSURANCE/contrats.csv"))
=======
        #.load("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/TEST_SONAR_GIT/DATA/SAMPLE/ABASSURANCE/contrats.csv")) # NOSONAR
        .load(f"{data_dir}/contrats.csv"))

        # df_ab_contrats = df_ab_contrats.withColumn("AB_PRIME_ANNUELLE",round(col("AB_PRIME_ANNUELLE").cast("double")/12,2))
>>>>>>> origin/Develop

        df_ab_sinistres = (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", "true")
<<<<<<< HEAD
        .load("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/TEST_SONAR_GIT/DATA/SAMPLE/ABASSURANCE/sinistres.csv"))
=======
        #.load("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/TEST_SONAR_GIT/DATA/SAMPLE/ABASSURANCE/sinistres.csv")) # NOSONAR
        .load(f"{data_dir}/sinistres.csv"))
>>>>>>> origin/Develop

        df_ab_payments = (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", "true")
<<<<<<< HEAD
        .load("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/TEST_SONAR_GIT/DATA/SAMPLE/ABASSURANCE/payments.csv"))
=======
        #.load("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/TEST_SONAR_GIT/DATA/SAMPLE/ABASSURANCE/payments.csv")) # NOSONAR
        .load(f"{data_dir}/payments.csv"))
>>>>>>> origin/Develop

        #  ACTION FORCÉE : On oblige Spark à analyser les données maintenant
        df_ab_clients.first() 
        df_ab_contrats.first() 
        df_ab_sinistres.first() 
        df_ab_payments.first() 
        
        print("Création dataframes réussie.")
        return df_ab_clients, df_ab_contrats, df_ab_sinistres, df_ab_payments
        
    except Exception as e:
        print("\n--- ÉCHEC DU PIPELINE ---")
        print("La création des dataframes Ab ssurance a échoué.")
        print(f"Détail technique de l'erreur : {e}")
        # On lève à nouveau l'erreur pour empêcher la suite du script de tourner avec dtaframes vides
        raise e

        


<<<<<<< HEAD
=======
# Ajout de sécurité du 30 0 9 2026
# from pyspark.sql import functions as F
# salt = "MonSelSecret2026!"
# df_ab_clients_protege = df_ab_clients.withColumn("Email_Hache", F.sha2(F.concat(F.col("AB_EMAIL"), # F.lit(salt)), 256))

"""
# Affichage des DataFrames
print("=== df_ab_clients ===")
df_ab_clients.show(10, truncate=False)

# ajout display de ab clinet protege de e mail le 30 0 9 2026
# print("=== df_ab_clients_protege ===")
# df_ab_clients_protege.show(10, truncate=False)


print("=== df_ab_contrats ===")
df_ab_contrats.show(10, truncate=False)

print("=== df_ab_sinistres ===")
df_ab_sinistres.show(10, truncate=False)

print("=== df_ab_payments ===")
df_ab_payments.show(10, truncate=False)

"""


import os

# Chemin relatif depuis l'emplacement du notebook dans le dépôt Git
file_path = os.path.join("DATA", "SAMPLE", "ABASSURANCE", "clients.csv")
df = spark.read.option("header", "true").csv(f"file:{os.path.abspath(file_path)}")


>>>>>>> origin/Develop

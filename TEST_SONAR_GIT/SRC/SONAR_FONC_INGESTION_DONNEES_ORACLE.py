"""
PARTIE 3 : IMPLEMENTATION TECHNIQUE SUR DATABRICKS (Sujet 3)
Dossier 1 : Structure Git et Modèle de Données
Question 1 : Repository Git
•	Branches : main (production), develop (intégration), feature/ingestion-oracle, feature/deduplication, bugfix/date-format.

•	Convention de nommage : <type>/<ticket-id>-<description-courte> (Ex: feature/AB-102-pipeline-sinistres).

Question 2 : Modèle Conceptuel des Données (MCD Unifié)
Le modèle réconcilie la structure d'AbAssurance (Oracle) et d'AssurePlus (SQL Server) : 


+-------------------------------------------------+
|                  DIM_CLIENT                     |
+-------------------------------------------------+
| master_client_id (PK)                           |
| source_system ('ORACLE' / 'SQL_SERVER')         |
| original_id                                     |
| nom, prenom, date_naissance, email, telephone   |
| adresse, code_postal, ville, pays               |
+-------------------------------------------------+
                        │ 1
                        
                        │ N
+-------------------------------------------------+
|                  FACT_CONTRAT                   |
+-------------------------------------------------+
| contrat_id (PK)                                 |
| master_client_id (FK)                           |
| type_contrat ('SANTE', 'AUTO', 'HABITATION'...) |
| date_debut, date_fin, statut                    |
| prime_annuelle_ttc                              |
+-------------------------------------------------+
                        │ 1
                        
                        │ N
+-------------------------------------------------+
|                  FACT_SINISTRE                  |
+-------------------------------------------------+
| sinistre_id (PK)                                |
| contrat_id (FK)                                 |
| date_sinistre, date_declaration                 |
| type_sinistre, statut ('OUVERT', 'CLOS'...)    |
| montant_indemnise, cout_estime                  |
+-------------------------------------------------+
Dossier 2 & 3 : Implémentation complète sous Databricks (PySpark)
"""
#   CONTRATs ET SIISTRES ET PAIEMENTS   et CLIENTS AUSSI    créer des tables silver 

#Étape 1 : Simulation des jeux de données Oracle & SQL Server et enregistrementen bronze

from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap, col, lower, upper, trim, concat_ws, levenshtein, when, lit, count, sum as _sum, avg, round, to_date
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
from datetime import datetime
from pyspark.sql.window import Window


from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, DoubleType

# Initialisation Spark
spark = SparkSession.builder.appName("AssuranceData").getOrCreate()


def create_ab_dataframes(spark):
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
        
        # Création des DataFrames avec les données
        ab_clients_data = [
            (1001, "MARTIN", "Jean", "1985-03-15", "jean.martin@gmail.fr", "0612345678", "15 Rue de Paris", "75001", "FR12345678901", "2020-01-15 10:30:00", "ACTIF"),
            (1002, "DUPONT", "Marie", "1990-07-22", "marie.dupont@gmail.fr", "0623456789", "22 Avenue des Lilas", "69002", "FR23456789012", "2020-03-20 14:15:00", "ACTIF"),
            (1003, "LEFEVRE", "Pierre", "1978-11-03", "pierre.lefevre@email.com", "0634567890", "8 Boulevard Victor Hugo", "13001", "FR34567890123", "2020-06-10 09:45:00", "ACTIF"),
            (1004, "BERNARD", "Sophie", "1992-05-18", "sophie.bernard@email.com", "0645678901", "35 Rue de la République", "44000", "FR45678901234", "2020-09-05 16:20:00", "ACTIF"),
            (1005, "THOMAS", "Luc", "1980-09-30", "luc.thomas@email.com", "0656789012", "12 Place de la Mairie", "31000", "FR56789012345", "2021-01-12 11:00:00", "ACTIF"),
            (1006, "PETIT", "Claire", "1995-12-12", "claire.petit@email.com", "0667890123", "45 Rue des Fleurs", "59000", "FR67890123456", "2021-04-18 08:30:00", "ACTIF"),
            (1007, "ROBERT", "Michel", "1975-06-25", "michel.robert@email.com", "0678901234", "18 Chemin des Vignes", "33000", "FR78901234567", "2021-07-22 13:45:00", "ACTIF"),
            (1008, "RICHARD", "Céline", "1988-02-14", "celine.richard@email.com", "0689012345", "56 Rue du Commerce", "76000", "FR89012345678", "2021-10-30 10:15:00", "ACTIF"),
            (1009, "DURAND", "Thomas", "1993-08-08", "thomas.durand@email.com", "0690123456", "7 Allée des Pins", "80000", "FR90123456789", "2022-02-14 15:30:00", "ACTIF"),
            (1010, "MOREAU", "Isabelle", "1982-04-20", "isabelle.moreau@email.com", "0601234567", "29 Rue Pasteur", "54000", "FR01234567890", "2022-05-20 12:00:00", "ACTIF")
        ]
        
        ab_contrats_data = [
            ("POL-2020-001", 1001, TYPE_AUTO, "2020-01-15", "2021-01-14", 850.50, "ACTIF", 101),
            ("POL-2020-002", 1002, TYPE_HABITATION, "2020-03-20", "2021-03-19", 420.30, "ACTIF", 102),
            ("POL-2020-003", 1003, TYPE_SANTE, "2020-06-10", "2021-06-09", 1250.00, "ACTIF", 103),
            ("POL-2020-004", 1004, TYPE_AUTO, "2020-09-05", "2021-09-04", 920.75, "ACTIF", 101),
            ("POL-2021-005", 1005, TYPE_HABITATION, "2021-01-12", "2022-01-11", 380.20, "ACTIF", 104),
            ("POL-2021-006", 1006, TYPE_AUTO, "2021-04-18", "2022-04-17", 780.00, "ACTIF", 102),
            ("POL-2021-007", 1007, TYPE_PREVOYANCE, "2021-07-22", "2022-07-21", 650.40, "ACTIF", 105),
            ("POL-2021-008", 1008, TYPE_SANTE, "2021-10-30", "2022-10-29", 1350.60, "ACTIF", 103),
            ("POL-2022-009", 1009, TYPE_AUTO, "2022-02-14", "2023-02-13", 895.25, "ACTIF", 101),
            ("POL-2022-010", 1010, TYPE_HABITATION, "2022-05-20", "2023-05-19", 410.90, "ACTIF", 104)
        ]
        
        ab_sinistres_data = [
            (2001, "POL-2020-001", "2020-06-15 14:30:00", 1250.00, "TRAITEMENT", "Accident de voiture, dégâts matériels"),
            (2002, "POL-2020-002", "2020-08-22 09:15:00", 350.00, "CLOS", "Dégât des eaux dans la cuisine"),
            (2003, "POL-2020-004", "2020-11-05 18:45:00", 2100.50, "TRAITEMENT", "Collision avec un autre véhicule"),
            (2004, "POL-2021-005", "2021-03-10 07:30:00", 175.30, "CLOS", "Vol de bijoux dans la maison"),
            (2005, "POL-2021-006", "2021-07-28 16:20:00", 980.00, "TRAITEMENT", "Bris de glace sur pare-brise"),
            (2006, "POL-2020-001", "2021-09-12 11:40:00", 1450.75, "EN_ATTENTE", "Incendie moteur, véhicule endommagé"),
            (2007, "POL-2021-008", "2022-01-18 13:10:00", 3200.00, "TRAITEMENT", "Hospitalisation suite à une chute"),
            (2008, "POL-2022-009", "2022-04-05 08:50:00", 560.20, "CLOS", "Rayure sur carrosserie, acte de vandalisme"),
            (2009, "POL-2021-007", "2022-07-14 20:00:00", 750.00, "TRAITEMENT", "Déclaration d'invalidité temporaire"),
            (2010, "POL-2022-010", "2022-10-31 10:25:00", 420.80, "EN_ATTENTE", "Infiltration d'eau dans le sous-sol")
        ]
        
        ab_payments_data = [
            (3001, "POL-2020-001", "2020-01-20", 850.50, MODE_CARTE_BANCAIRE),
            (3002, "POL-2020-002", "2020-03-25", 420.30, MODE_PRELEVEMENT),
            (3003, "POL-2020-003", "2020-06-15", 625.00, MODE_CARTE_BANCAIRE),
            (3004, "POL-2020-004", "2020-09-10", 920.75, MODE_VIREMENT),
            (3005, "POL-2021-005", "2021-01-15", 380.20, MODE_PRELEVEMENT),
            (3006, "POL-2021-006", "2021-04-20", 780.00, MODE_CARTE_BANCAIRE),
            (3007, "POL-2021-007", "2021-07-25", 650.40, MODE_CHEQUE),
            (3008, "POL-2021-008", "2021-11-05", 675.30, MODE_CARTE_BANCAIRE),
            (3009, "POL-2022-009", "2022-02-18", 895.25, MODE_VIREMENT),
            (3010, "POL-2022-010", "2022-05-25", 410.90, MODE_PRELEVEMENT)
        ]
        
        # création des dataframes
        print("Création des Ab dataframes")
        df_ab_clients = spark.createDataFrame(ab_clients_data, schema=ab_clients_schema)
        df_ab_contrats = spark.createDataFrame(ab_contrats_data, schema=ab_contrats_schema)
        #Transformation de la prime annuelle en prime mensuelle
        df_ab_contrats = df_ab_contrats.withColumn("AB_PRIME_ANNUELLE",round(col("AB_PRIME_ANNUELLE").cast("double")/12,2))

        df_ab_sinistres = spark.createDataFrame(ab_sinistres_data, schema=ab_sinistres_schema)
        df_ab_payments = spark.createDataFrame(ab_payments_data, schema=ab_payments_schema)
        
    
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


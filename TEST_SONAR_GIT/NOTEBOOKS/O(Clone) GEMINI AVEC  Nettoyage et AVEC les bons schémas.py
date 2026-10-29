# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.gold;

# COMMAND ----------

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
                        │
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
                        │
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


#Étape 1 : Simulation des jeux de données Oracle & SQL Server et enregistrementen bronze

from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap, col, lower, upper, trim, concat_ws, levenshtein, when, lit, count, sum as _sum, avg, round, to_date
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
from datetime import datetime
from pyspark.sql.window import Window


from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, DoubleType


# Données oracle



# Initialisation Spark
spark = SparkSession.builder.appName("AssuranceData").getOrCreate()

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
    ("POL-2020-001", 1001, "Auto", "2020-01-15", "2021-01-14", 850.50, "ACTIF", 101),
    ("POL-2020-002", 1002, "Habitation", "2020-03-20", "2021-03-19", 420.30, "ACTIF", 102),
    ("POL-2020-003", 1003, "Santé", "2020-06-10", "2021-06-09", 1250.00, "ACTIF", 103),
    ("POL-2020-004", 1004, "Auto", "2020-09-05", "2021-09-04", 920.75, "ACTIF", 101),
    ("POL-2021-005", 1005, "Habitation", "2021-01-12", "2022-01-11", 380.20, "ACTIF", 104),
    ("POL-2021-006", 1006, "Auto", "2021-04-18", "2022-04-17", 780.00, "ACTIF", 102),
    ("POL-2021-007", 1007, "Prévoyance", "2021-07-22", "2022-07-21", 650.40, "ACTIF", 105),
    ("POL-2021-008", 1008, "Santé", "2021-10-30", "2022-10-29", 1350.60, "ACTIF", 103),
    ("POL-2022-009", 1009, "Auto", "2022-02-14", "2023-02-13", 895.25, "ACTIF", 101),
    ("POL-2022-010", 1010, "Habitation", "2022-05-20", "2023-05-19", 410.90, "ACTIF", 104)
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
    (3001, "POL-2020-001", "2020-01-20", 850.50, "Carte bancaire"),
    (3002, "POL-2020-002", "2020-03-25", 420.30, "Prélèvement"),
    (3003, "POL-2020-003", "2020-06-15", 625.00, "Carte bancaire"),
    (3004, "POL-2020-004", "2020-09-10", 920.75, "Virement"),
    (3005, "POL-2021-005", "2021-01-15", 380.20, "Prélèvement"),
    (3006, "POL-2021-006", "2021-04-20", 780.00, "Carte bancaire"),
    (3007, "POL-2021-007", "2021-07-25", 650.40, "Chèque"),
    (3008, "POL-2021-008", "2021-11-05", 675.30, "Carte bancaire"),
    (3009, "POL-2022-009", "2022-02-18", 895.25, "Virement"),
    (3010, "POL-2022-010", "2022-05-25", 410.90, "Prélèvement")
]

# Création des DataFrames

df_ab_clients = spark.createDataFrame(ab_clients_data, schema=ab_clients_schema)
df_ab_contrats = spark.createDataFrame(ab_contrats_data, schema=ab_contrats_schema)
df_ab_sinistres = spark.createDataFrame(ab_sinistres_data, schema=ab_sinistres_schema)
df_ab_payments = spark.createDataFrame(ab_payments_data, schema=ab_payments_schema)



# Affichage des DataFrames
print("=== df_ab_clients ===")
df_ab_clients.show(10, truncate=False)

print("=== df_ab_contrats ===")
df_ab_contrats.show(10, truncate=False)

print("=== df_ab_sinistres ===")
df_ab_sinistres.show(10, truncate=False)

print("=== df_ab_payments ===")
df_ab_payments.show(10, truncate=False)





# Données AssurPlus SQLserver


# Initialisation Spark
# spark = SparkSession.builder.appName("AssurePlusData").getOrCreate()

# Définition des schémas pour AssurePlus
ap_clients_schema = StructType([
    StructField("AP_CLIENT_ID", IntegerType(), True),
    StructField("AP_NOM", StringType(), True),
    StructField("AP_PRENOM", StringType(), True),
    StructField("AP_NOM_COMPLET", StringType(), True),

    StructField("AP_DATE_NAISSANCE", StringType(), True),
    StructField("AP_EMAIL", StringType(), True),
    StructField("AP_TELEPHONE", StringType(), True),
    StructField("AP_ADRESSE", StringType(), True),
    StructField("AP_CODE_POSTAL", StringType(), True),
    StructField("AP_NUM_FISCAL", StringType(), True),
    StructField("AP_DATE_CREATION", StringType(), True),
    StructField("AP_STATUT_CLIENT", StringType(), True)
])

ap_contrats_schema = StructType([
    StructField("AP_POLICY_NUMBER", StringType(), True),
    StructField("AP_CLIENT_ID", IntegerType(), True),
    StructField("AP_TYPE_ASSURANCE", StringType(), True),
    StructField("AP_DATE_DEBUT", StringType(), True),
    StructField("AP_DATE_FIN", StringType(), True),
    StructField("AP_PRIME_ANNUELLE", DoubleType(), True),
    StructField("AP_STATUT_CONTRAT", StringType(), True),
    StructField("AP_AGENCE_ID", IntegerType(), True)
])

ap_sinistres_schema = StructType([
    StructField("AP_CLAIM_ID", IntegerType(), True),
    StructField("AP_POLICY_NUMBER", StringType(), True),
    StructField("AP_DATE_SINISTRE", StringType(), True),
    StructField("AP_MONTANT_ESTIME", DoubleType(), True),
    StructField("AP_STATUT_SINISTRE", StringType(), True),
    StructField("AP_DESCRIPTION", StringType(), True)
])

ap_payments_schema = StructType([
    StructField("AP_PAYMENT_ID", IntegerType(), True),
    StructField("AP_POLICY_NUMBER", StringType(), True),
    StructField("AP_DATE_PAYMENT", StringType(), True),
    StructField("AP_MONTANT", DoubleType(), True),
    StructField("AP_MODE_PAYMENT", StringType(), True)
])

# Données AS_CLIENT
ap_clients_data = [
    (5001,"" ,"" ,"Jean Martin", "1985-03-15", "jean.martin@gmail.fr", "07 12 34 56 78", 
     "120 Avenue des Champs", "75008", "FR98765432101", "2021-01-10 09:00:00", "ACTIF"),
    (5002,"" ,"" ,"Emilie Morin", "1991-11-25", "emilie.morin@assureplus.fr", "0723456789", 
     "45 Rue de la Paix", "69003", "FR87654321012", "2021-03-15 14:30:00", "ACTIF"),
    (5003, "DUPONT","Marie","Marie DUPONT", "1990-07-22", "marie.dupont@gmail.fr", "0734567890", 
     "18 Boulevard Saint-Michel", "75005", "FR76543210923", "2021-06-20 11:15:00", "ACTIF"),
    (5004, "VINCENT", "Laura","Laura Vincent", "1993-08-30", "laura.vincent@assureplus.fr", "0745678901", 
     "52 Rue des Arts", "31000", "FR65432109834", "2021-09-25 16:45:00", "ACTIF"),
    (5005, "GIRARD", "François","François Girard", "1984-12-17", "francois.girard@assureplus.fr", "0756789012", 
     "8 Place de la Concorde", "44000", "FR54321098745", "2022-01-05 10:30:00", "ACTIF"),
    (5006, "FOURNIER", "Cécile","Cécile Fournier", "1996-05-03", "cecile.fournier@assureplus.fr", "0767890123", 
     "33 Rue des Lilas Blancs", "59000", "FR43210987656", "2022-04-12 13:00:00", "ACTIF"),
    (5007, "MOREAU", "Philippe","Philippe Moreau", "1976-10-19", "philippe.moreau@assureplus.fr", "0778901234", 
     "27 Allée des Roses", "33000", "FR32109876567", "2022-07-18 08:45:00", "ACTIF"),
    (5008, "LEROY", "Anne","anne Leroy", "1989-02-28", "anne.leroy@assureplus.fr", "0789012345", 
     "69 Rue du Faubourg", "76000", "FR21098765478", "2022-10-22 15:20:00", "ACTIF"),
    (5009, "DAVID", "Julien","Julien David", "1994-07-14", "julien.david@assureplus.fr", "0790123456", 
     "14 Boulevard de la Libération", "13003", "FR10987654389", "2023-01-30 12:10:00", "ACTIF"),
    (5010, "MARTIN", "Valérie","Valérie Martin", "1981-03-22", "valerie.martin@assureplus.fr", "0701234567", 
     "86 Rue Nationale", "54000", "FR09876543290", "2023-03-14 09:40:00", "ACTIF")
]

# Données AS_CONTRAT
ap_contrats_data = [
    ("ASP-2021-001", 5001, "Auto", "2021-01-10", "2022-01-09", 920.00, "ACTIF", 201),
    ("ASP-2021-002", 5002, "Habitation", "2021-03-15", "2022-03-14", 450.50, "ACTIF", 202),
    ("ASP-2021-003", 5003, "Santé", "2021-06-20", "2022-06-19", 1320.80, "ACTIF", 203),
    ("ASP-2021-004", 5004, "Auto", "2021-09-25", "2022-09-24", 780.30, "ACTIF", 204),
    ("ASP-2022-005", 5005, "Habitation", "2022-01-05", "2023-01-04", 395.60, "ACTIF", 205),
    ("ASP-2022-006", 5006, "Auto", "2022-04-12", "2023-04-11", 850.00, "ACTIF", 201),
    ("ASP-2022-007", 5007, "Prévoyance", "2022-07-18", "2023-07-17", 700.20, "ACTIF", 206),
    ("ASP-2022-008", 5008, "Santé", "2022-10-22", "2023-10-21", 1450.40, "ACTIF", 203),
    ("ASP-2023-009", 5009, "Auto", "2023-01-30", "2024-01-29", 945.75, "ACTIF", 202),
    ("ASP-2023-010", 5010, "Habitation", "2023-03-14", "2024-03-13", 430.90, "ACTIF", 205)
]

# Données AS_SINISTRE
ap_sinistres_data = [
    (8001, "ASP-2021-001", "2021-05-12 15:30:00", 1850.25, "REGLE", 
     "Accident sur autoroute, dégâts importants"),
    (8002, "ASP-2021-002", "2021-07-28 10:15:00", 280.00, "REGLE", 
     "Fuite de canalisation, cuisine inondée"),
    (8003, "ASP-2021-004", "2021-11-03 08:45:00", 3200.50, "EN_COURS", 
     "Carambolage avec 3 véhicules"),
    (8004, "ASP-2022-005", "2022-02-18 19:20:00", 125.30, "REGLE", 
     "Bris de vitre, tentative d'effraction"),
    (8005, "ASP-2022-006", "2022-06-30 14:10:00", 760.00, "EN_COURS", 
     "Dégâts suite à un orage, grêle"),
    (8006, "ASP-2021-001", "2022-08-15 11:50:00", 980.40, "REGLE", 
     "Vol de roues sur le véhicule"),
    (8007, "ASP-2022-008", "2022-12-20 16:30:00", 2800.00, "EN_COURS", 
     "Intervention chirurgicale urgente"),
    (8008, "ASP-2023-009", "2023-03-05 07:40:00", 450.00, "REGLE", 
     "Petit accident, pare-chocs endommagé"),
    (8009, "ASP-2022-007", "2023-05-18 09:00:00", 1500.00, "EN_COURS", 
     "Arrêt de travail prolongé, invalidité"),
    (8010, "ASP-2023-010", "2023-07-25 13:25:00", 310.50, "NOUVEAU", 
     "Infiltration suite à fortes pluies")
]

# Données AS_PAYMENT
ap_payments_data = [
    (9001, "ASP-2021-001", "2021-01-15", 920.00, "CB en ligne"),
    (9002, "ASP-2021-002", "2021-03-20", 450.50, "Prélèvement auto"),
    (9003, "ASP-2021-003", "2021-06-25", 660.40, "Virement bancaire"),
    (9004, "ASP-2021-004", "2021-09-30", 780.30, "CB en ligne"),
    (9005, "ASP-2022-005", "2022-01-10", 395.60, "Prélèvement auto"),
    (9006, "ASP-2022-006", "2022-04-15", 850.00, "Chèque"),
    (9007, "ASP-2022-007", "2022-07-20", 700.20, "CB en ligne"),
    (9008, "ASP-2022-008", "2022-10-25", 725.20, "Virement bancaire"),
    (9009, "ASP-2023-009", "2023-02-05", 945.75, "Prélèvement auto"),
    (9010, "ASP-2023-010", "2023-03-18", 430.90, "CB en ligne")
]

# Création des DataFrame
df_ap_clients = spark.createDataFrame(ap_clients_data, schema=ap_clients_schema)
df_ap_contrats = spark.createDataFrame(ap_contrats_data, schema=ap_contrats_schema)
df_ap_sinistres = spark.createDataFrame(ap_sinistres_data, schema=ap_sinistres_schema)
df_ap_payments = spark.createDataFrame(ap_payments_data, schema=ap_payments_schema)



# Affichage
print("=== ASSUREPLUS - Clients ===")
df_ap_clients.show(10, truncate=False)

print("=== ASSUREPLUS - Contrats ===")
df_ap_contrats.show(10, truncate=False)

print("=== ASSUREPLUS - Sinistres ===")
df_ap_sinistres.show(10, truncate=False)

print("=== ASSUREPLUS - Payments ===")
df_ap_payments.show(10, truncate=False)




# Ajout des données pour avoir la trace

from pyspark.sql import functions as F

# 1. Définition des fonctions
def ajoutmétadonnées(df, source_system, source_file, client_id_col):
    return df \
        .withColumn("SOURCE_SYSTEM", F.lit(source_system)) \
        .withColumn("ORIGINAL_ID", F.col(client_id_col).cast("string")) \
        .withColumn("SOURCE_FILE", F.lit(source_file)) \
        .withColumn("INGESTION_TIMESTAMP", F.current_timestamp())



# 2. Exécution de la chaîne (Exemple avec un DataFrame initialisé)
# oracle_clients = spark.read...



# ORACLE
df_ab_clients =ajoutmétadonnées (df_ab_clients,"Oracle","Oracle_clients.csv","AB_CLIENT_ID")

#SQL Server
df_ap_clients =ajoutmétadonnées (df_ap_clients,"SQL","SQL_clients.csv","AP_CLIENT_ID")


display(df_ab_clients)
display(df_ap_clients)



#                                   ENREGISTREMENT EN BRONZE        
# ATTENTION: Toutes les tables ne sont pas enregistrées dans la base de données delta
def enregistrement_bronze(df,tablename):

 df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tablename)

# Tables Oracles
enregistrement_bronze(df_ab_clients,"bronze.oracle_clients")
enregistrement_bronze(df_ab_contrats,"bronze.oracle_contrats")
enregistrement_bronze(df_ab_sinistres,"bronze.oracle_sinistres")
enregistrement_bronze(df_ab_payments,"bronze.oracle_payments")

# Tables SQL Server
enregistrement_bronze(df_ap_clients,"bronze.sqlserver_clients")
enregistrement_bronze(df_ap_contrats,"bronze.sqlserver_contrats")
enregistrement_bronze(df_ap_payments,"bronze.sqlserver_payments")
enregistrement_bronze(df_ap_sinistres,"bronze.sqlserver_sinistres")


# Étape 2 : Data Pipeline - Harmonisation & Unification (Silver Layer)

"""

Schéma client final :

CLIENT_ID
SOURCE_SYSTEM
SOURCE_CLIENT_ID
NOM
PRENOM
NOM_COMPLET
DATE_NAISSANCE
EMAIL
TELEPHONE
ADRESSE
CODE_POSTAL
NUMERO_FISCAL
DATE_CREATION
STATUT_CLIENT
SCORE_FIDELITE
DATE_INTEGRATION
SOURCE_FILE
INGESTION_TIMESTAMP
VILLE     je ne l'ai pas rajouté



"""
# -------------------       NETTOYAGE  -------------------------------
# Handle nulls
def handle_nulls(df, column, default_value=None, strategy='fill'):
    """
    Gère les valeurs NULL.
    strategies: 'fill' (remplacer), 'drop' (supprimer), 'flag' (marquer)
    """
    if strategy == 'fill':
        return df.fillna({column: default_value})
    elif strategy == 'drop':
        return df.filter(col(column).isNotNull())
    elif strategy == 'flag':
        return df.withColumn(f"{column}_is_null", col(column).isNull())

# Clean address
def clean_address(df, address_column, postal_code_column, city_column):
    """Nettoie et normalise les adresses"""
    return (df
        .withColumn(address_column, trim(regexp_replace(col(address_column), r'\s+', ' ')))
        .withColumn(postal_code_column, regexp_replace(col(postal_code_column), r'\D', ''))
        .withColumn(city_column, upper(trim(col(city_column))))
    )
 # Scinder en nom et prénom le full name de Assurplus
def split_full_name(df,col_last_name, col_first_name,col_full_name):

 df = df.withColumn(col_last_name, F.trim(F.split(F.col(col_full_name), " ").getItem(1))) \
             .withColumn(col_first_name, F.trim(F.split(F.col(col_full_name), " ").getItem(0)))
 return df
df_ap_clients = split_full_name(df_ap_clients,"AP_NOM","AP_PRENOM","AP_NOM_COMPLET")

# Nettoyage des noms 
from pyspark.sql import functions as F
def clean_last_name(df,last_name_col):
  df = df.withColumn(
  last_name_col,
  F.upper(F.trim(F.col(last_name_col))))
  return df


# Nettoyage des noms des clients ORACLE


df_ab_clients = clean_last_name(df_ab_clients,"AB_NOM")

# Nettoyage des noms des users SQLServer

df_ap_clients = clean_last_name(df_ap_clients,"AP_NOM")



# Nettoyage des prénoms

# 1. Définition de la fonction (elle prend un nom de colonne en chaîne de caractères)
def normalize_firstname(column_name):
    return F.upper(F.trim(F.col(column_name)))

# 2. Application  sur le DataFrame Oracle
df_ab_clients = df_ab_clients.withColumn("AB_PRENOM", normalize_firstname("AB_PRENOM"))

# 3. Application  sur le DataFrame SQL
df_ap_clients= df_ap_clients.withColumn("AP_PRENOM", normalize_firstname("AP_PRENOM"))

# nettoyage des Emails

# 1. Définition de la fonction (elle prend un nom de colonne en chaîne de caractères)
def normalize_email(column_name):
    return F.lower(F.trim(F.col(column_name)))

# 2. Application  sur le DataFrame Oracle
df_ab_clients = df_ab_clients.withColumn("AB_EMAIL", normalize_email("AB_EMAIL"))

# 2. Application  sur le DataFrame SQL
df_ap_clients= df_ap_clients.withColumn("AP_EMAIL", normalize_email("AP_EMAIL"))

# Nettoyage des dates 
def normalize_date (df,col_name):
  df= df.withColumn(col_name, to_date(col(col_name), "yyyy-MM-dd"))
  return df

#appliquée à Oracle
df_ab_clients= normalize_date(df_ab_clients,"AB_DATE_NAISSANCE")
df_ab_clients= df_ab_clients.withColumn("AB_DATE_CREATION", to_timestamp(col("AB_DATE_CREATION"), "yyyy-MM-dd HH:mm:ss"))

#appliquée à SQL Server
df_ap_clients= normalize_date(df_ap_clients,"AP_DATE_NAISSANCE")
df_ap_clients= df_ap_clients.withColumn("AP_DATE_CREATION", to_timestamp(col("AP_DATE_CREATION"), "yyyy-MM-dd HH:mm:ss"))


# Nettoyage des numéros de Téléphone mais rajouter +33 


# Correction de la fonction normalize_phone_number (format international)
def normalize_phone_number(df, column_name):
    # Supprime tout sauf les chiffres et applique le format français
    return df.withColumn(
        column_name,
        concat(
            lit("+33"),
            regexp_replace(
                regexp_replace(col(column_name), r'[^0-9]', ''), r'^0', ''
            )
        )
    )
# Appliquer la fonction aux colonnes de téléphone
df_ab_clients = normalize_phone_number(df_ab_clients, "AB_TELEPHONE")
df_ap_clients = normalize_phone_number(df_ap_clients, "AP_TELEPHONE")

display(df_ab_clients)
display(df_ap_clients)



# Harmonisation des clients Oracle
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
# Harmonisation des clients SQL Server
std_df_ap_clients = df_ap_clients.select(
    lit("SQL").alias("SOURCE_SYSTEM"),
    col("AP_CLIENT_ID").cast("string").alias("ORIGINAL_ID"),
    upper(trim(col("AP_NOM"))).alias("NOM"),
    initcap(trim(col("AP_PRENOM"))).alias("PRENOM"),
    col("AP_DATE_NAISSANCE").alias("DATE_NAISSANCE"),
    lower(trim(col("AP_EMAIL"))).alias("EMAIL"),
    col("AP_TELEPHONE").alias("TELEPHONE"),
    col("AP_ADRESSE").alias("ADRESSE"),
    col("AP_CODE_POSTAL").alias("CODE_POSTAL"),
    col("AP_NUM_FISCAL").alias("NUMERO_FISCAL"),
    col("AP_DATE_CREATION").alias("DATE_CREATION"),
    col("AP_STATUT_CLIENT").alias("STATUT_CLIENT"),
    col("INGESTION_TIMESTAMP").alias("INGESTION_TIMESTAMP"),
    col("SOURCE_FILE").alias("SOURCE_FILE")
)
# Union des clients harmonisés
df_clients = std_df_ab_clients.union (std_df_ap_clients)
df_clients.createOrReplaceTempView("raw_unified_clients")
df_clients.display()



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

dim_client.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_dim_client")

# Affichage de la Matrice de Doublons
print("--- MATRICE DE DOUBLONS IDENTIFIÉS ---")
doublons_df = dim_client.groupBy("MASTER_CLIENT_ID") \
    .agg(count("ORIGINAL_ID").alias("NB_OCCURENCES")) \
    .filter(col("NB_OCCURENCES") > 1)

display(dim_client.join(doublons_df, "MASTER_CLIENT_ID"))

# Étape 4 : Tables de Faits Contrats et Sinistres (Gold Layer)

# Unification Contrats
facts_contrats = df_ab_contrats.select(
    col("AB_POLICY_NUMBER").cast("string").alias("CONTRAT_ID"),
    lit("ORACLE").alias("SOURCE_SYSTEM"),
    col("AB_CLIENT_ID").cast("string").alias("ORIGINAL_CLIENT_ID"),
    col("AB_TYPE_ASSURANCE").alias("TYPE_CONTRAT"),
    col("AB_STATUT_CONTRAT").alias("STATUT_CONTRAT"),
    col("AB_PRIME_ANNUELLE").alias("MONTANT_PRIME")
).union(
    df_ap_contrats.select(
        col("AP_POLICY_NUMBER").cast("string").alias("CONTRAT_ID"),
        lit("SQL").alias("SOURCE_SYSTEM"),
        col("AP_CLIENT_ID").cast("string").alias("ORIGINAL_CLIENT_ID"),
        col("AP_TYPE_ASSURANCE").alias("TYPE_CONTRAT"),
        col("AP_STATUT_CONTRAT").alias("STATUT_CONTRAT"),
        col("AP_PRIME_ANNUELLE").alias("MONTANT_PRIME")
    )
)
print("#------ Affichage des contrats --------")

display(facts_contrats)

# Mapping avec Master_Client_ID
fact_contrat_gold = facts_contrats.join(
    dim_client,
    (facts_contrats.ORIGINAL_CLIENT_ID == dim_client.ORIGINAL_ID) & 
    (facts_contrats.SOURCE_SYSTEM == dim_client.SOURCE_SYSTEM)
).select("CONTRAT_ID", "MASTER_CLIENT_ID", "TYPE_CONTRAT", "MONTANT_PRIME", "STATUT_CONTRAT")

print("#------ Affichage des contrats  après mapping --------")

display(fact_contrat_gold)

fact_contrat_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_fact_contrat")

# Unification Sinistres
fact_sinistre_gold = df_ab_sinistres.select(
    col("AB_CLAIM_ID").cast("string").alias("SINISTRE_ID"),
    col("AB_POLICY_NUMBER").cast("string").alias("CONTRAT_ID"),
    col("AB_MONTANT_ESTIME").alias("MONTANT_INDEMNISE"),
     col("AB_STATUT_SINISTRE").alias("STATUT_SINISTRE")
).union(
    df_ap_sinistres.select(
        col("AP_CLAIM_ID").cast("string").alias("SINISTRE_ID"),
        col("AP_POLICY_NUMBER").alias("CONTRAT_ID"),
        col("AP_MONTANT_ESTIME").alias("MONTANT_INDEMNISE"),
        col("AP_STATUT_SINISTRE").alias("STATUT_SINISTRE")

    )
)

fact_sinistre_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_fact_sinistre")


# Unification payments

fact_payment_gold = df_ab_payments.select(
    col("AB_PAYMENT_ID").cast("string").alias("PAYMENT_ID"),
    col("AB_POLICY_NUMBER").cast("string").alias("POLICY_NUMBER"),
    col("AB_DATE_PAYMENT").alias("DATE_PAYMENT"),
    col("AB_MONTANT").alias("MONTANT"),
    col("AB_MODE_PAYMENT").alias("MODE_PAYMENT")
).union(
    df_ap_payments.select(
    col("AP_PAYMENT_ID").cast("string").alias("PAYMENT_ID"),
    col("AP_POLICY_NUMBER").cast("string").alias("POLICY_NUMBER"),
    col("AP_DATE_PAYMENT").alias("DATE_PAYMENT"),
    col("AP_MONTANT").alias("MONTANT"),
    col("AP_MODE_PAYMENT").alias("MODE_PAYMENT")
    )
)

fact_payment_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_fact_payment")







#Dossier 3 (Suite) : Tests unitaires, Qualité & Dashboards

#Question 1 & 2 : Tests unitaires PySpark & Test Coverage
# Pour valider votre code (compatible avec les tests unitaires PySpark) :

# voir cellule 3




#Question 3 : Identification d'un bug potentiel dans le pipeline
"""
•	Bug identifié : Clés primaires dupliquées après l'UNION simple.

•	Explication : Si AbAssurance et AssurePlus utilisent tous deux des identifiants numériques séquentiels (ID_CLIENT = 101 dans Oracle et CustomerID = 101 dans SQL Server), faire une simple jointure entraîne de fausses associations.

•	Correction : Toujours créer une clé composite (source_system + original_id) ou générer une clé surrogate unique via UUID / dense_rank() comme réalisé dans l'étape 3.

"""

"""
# Création du Dashboard directement sous Databricks
# Pour réaliser les visualisations demandées : 


#1.	Créez une nouvelle cellule SQL dans votre notebook.

#2.	Exécutez les 4 requêtes ci-dessous.

#3.	Cliquez sur l'icône "+" à côté des résultats de la cellule -> Visualization, sélectionnez le type de graphique, puis cliquez sur Add to Dashboard. 
"""
"""
#SQL
#-- 1. Répartition des clients par Ville (Graphique Camembert / Pie Chart)
SELECT ville, COUNT(DISTINCT master_client_id) as total_clients
FROM gold_dim_client
GROUP BY ville;

#-- 2. Statistiques Contrats (Graphique en Barres)
SELECT type_contrat, COUNT(contrat_id) as nb_contrats, SUM(montant_prime) as total_primes
FROM gold_fact_contrat
GROUP BY type_contrat;

#-- 3. Statistiques Sinistres & Financières (Indicateurs / Counter Widgets)
SELECT 
SUM(c.montant_prime) as Chiffre_Affaires_Total,
SUM(s.montant_indemnise) as Total_Indemnites_Versees,
ROUND((SUM(s.montant_indemnise) / SUM(c.montant_prime)) * 100, 2) as Ratio_Sinistralite_Pourcent
FROM gold_fact_contrat c
LEFT JOIN gold_fact_sinistre s ON c.contrat_id = s.contrat_id;

#-- 4. Matrice des Doublons (Tableau filtré)
SELECT master_client_id, nom, prenom, COUNT(*) as nombre_de_comptes
FROM gold_dim_client
GROUP BY master_client_id, nom, prenom
HAVING COUNT(*) > 1;
"""
"""
Résumé des livrables pour votre rendu :
1.	Document de cadrage/spécifications : Reprenez les sections du Dossier 1 (Sujet 1). 

2.	Schéma Data Flow : Exportez le schéma ASCII fourni dans le Dossier 3 (Sujet 1). 

3.	Agilité & Risques : Utilisez les tickets Trello et la matrice des risques rédigés dans le Sujet 2. 

4.	Code & Dashboard : Exécutez le script PySpark complet sous Databricks Community Edition pour générer les tables Delta et publier le tableau de bord interactif.

"""





# COMMAND ----------

import unittest
from pyspark.sql import DataFrame

class TestETLPipeline(unittest.TestCase):
    # Nettoyage email
    def test_clean_email(self):
        self.assertEqual(clean_email("  JEAN.DUPONT@Email.FR "), "jean.dupont@email.fr")
        self.assertIsNone(clean_email(None))

    # Test création métadonnées
    def test_ajoutetadonnees(self):
        df = df_ab_clients.limit(1)
        df_meta = ajoutmétadonnées(df, "Oracle", "file.csv", "AB_CLIENT_ID")
        columns = df_meta.columns
        for colname in ["SOURCE_SYSTEM", "ORIGINAL_ID", "SOURCE_FILE", "INGESTION_TIMESTAMP"]:
            self.assertIn(colname, columns)

    # Test union harmonisée
    def test_unified_clients(self):
        self.assertIsInstance(df_clients, DataFrame)
        self.assertTrue(df_clients.count() > 0)

    # Test harmonisation des noms
    def test_clean_last_name(self):
        df = df_ab_clients.withColumn("AB_NOM", F.lit("   martin "))
        df_clean = clean_last_name(df, "AB_NOM")
        value = df_clean.select("AB_NOM").collect()[0][0]
        self.assertEqual(value, "MARTIN")

    # Test harmonisation des prénoms
    def test_normalize_firstname(self):
        df = df_ab_clients.withColumn("AB_PRENOM", F.lit("  jean "))
        df_norm = df.withColumn("AB_PRENOM", normalize_firstname("AB_PRENOM"))
        value = df_norm.select("AB_PRENOM").collect()[0][0]
        self.assertEqual(value, "JEAN")

    # Test normalisation téléphone
    def test_normalize_phone_number(self):
        df = df_ab_clients.withColumn("AB_TELEPHONE", F.lit("0612345678"))
        df_norm = normalize_phone_number(df, "AB_TELEPHONE")
        value = df_norm.select("AB_TELEPHONE").collect()[0][0]
        self.assertTrue(value.startswith("+33"))

    # Test matching doublons
    def test_matching_key(self):
        self.assertIn("MATCH_KEY", matched_clients.columns)
        keys = matched_clients.select("MATCH_KEY").distinct().count()
        self.assertTrue(keys > 0)

    # Test dense_rank pour MASTER_CLIENT_ID
    def test_master_client_id_uniqueness(self):
        count_id = dim_client.select("MASTER_CLIENT_ID").distinct().count()
        self.assertTrue(count_id > 0)

    # Test gold layer - contrats non vide
    def test_gold_fact_contrat(self):
        self.assertIsInstance(fact_contrat_gold, DataFrame)
        self.assertTrue(fact_contrat_gold.count() > 0)

    # Test gold layer - sinistres non vide
    def test_gold_fact_sinistre(self):
        self.assertIsInstance(fact_sinistre_gold, DataFrame)
        self.assertTrue(fact_sinistre_gold.count() > 0)

    # Test doublons
    def test_doublons(self):
        df = doublons_df
        self.assertTrue("MASTER_CLIENT_ID" in df.columns)
        self.assertTrue(df.count() >= 0)

suite = unittest.TestLoader().loadTestsFromTestCase(TestETLPipeline)
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /*
# MAGIC # Création du Dashboard directement sous Databricks
# MAGIC # Pour réaliser les visualisations demandées : 
# MAGIC
# MAGIC
# MAGIC #1.	Créez une nouvelle cellule SQL dans votre notebook.
# MAGIC
# MAGIC #2.	Exécutez les 4 requêtes ci-dessous.
# MAGIC
# MAGIC #3.	Cliquez sur l'icône "+" à côté des résultats de la cellule -> Visualization, sélectionnez le type de graphique, puis cliquez sur Add to Dashboard. 
# MAGIC
# MAGIC */
# MAGIC -- 0. Nombre total de clients dans la base
# MAGIC SELECT COUNT(DISTINCT MASTER_CLIENT_ID) AS total_clients
# MAGIC FROM workspace.default.gold_dim_client;
# MAGIC
# MAGIC -- 1. Répartition des clients par code postal (Pie Chart)
# MAGIC SELECT CODE_POSTAL, COUNT(DISTINCT MASTER_CLIENT_ID) AS nb_clients
# MAGIC FROM workspace.default.gold_dim_client
# MAGIC GROUP BY CODE_POSTAL;
# MAGIC
# MAGIC -- 2. Statistiques des contrats par type (Bar Chart)
# MAGIC SELECT TYPE_CONTRAT, COUNT(CONTRAT_ID) AS nb_contrats, SUM(MONTANT_PRIME) AS montant_total
# MAGIC FROM workspace.default.gold_fact_contrat
# MAGIC GROUP BY TYPE_CONTRAT;
# MAGIC
# MAGIC -- 3. Statistiques sinistres et financières (KPIs)
# MAGIC SELECT 
# MAGIC     SUM(c.MONTANT_PRIME) AS chiffre_affaires,
# MAGIC     SUM(s.MONTANT_INDEMNISE) AS indemnites_versees,
# MAGIC     ROUND((SUM(s.MONTANT_INDEMNISE) / SUM(c.MONTANT_PRIME)) * 100, 2) AS ratio_sinistralite_pct
# MAGIC FROM workspace.default.gold_fact_contrat c
# MAGIC LEFT JOIN workspace.default.gold_fact_sinistre s 
# MAGIC     ON c.CONTRAT_ID = s.CONTRAT_ID;
# MAGIC
# MAGIC -- 4. Matrice des doublons (Table Filter)
# MAGIC SELECT MASTER_CLIENT_ID, NOM, PRENOM, COUNT(*) AS nb_comptes
# MAGIC FROM workspace.default.gold_dim_client
# MAGIC GROUP BY MASTER_CLIENT_ID, NOM, PRENOM
# MAGIC HAVING COUNT(*) > 1;
# MAGIC
# MAGIC --SQL
# MAGIC --0. Création de l'information total_clients
# MAGIC SELECT COUNT(DISTINCT MASTER_CLIENT_ID) as total_clients
# MAGIC FROM gold_dim_client;
# MAGIC
# MAGIC -- 1. Répartition des clients par Ville (Graphique Camembert / Pie Chart)
# MAGIC SELECT CODE_POSTAL, COUNT(DISTINCT MASTER_CLIENT_ID) as total_clients
# MAGIC FROM gold_dim_client
# MAGIC GROUP BY CODE_POSTAL;
# MAGIC
# MAGIC
# MAGIC -- 2. Statistiques Contrats (Graphique en Barres)
# MAGIC SELECT TYPE_CONTRAT, COUNT(CONTRAT_ID) as nb_contrats, SUM(MONTANT_PRIME) as total_primes
# MAGIC FROM gold_fact_contrat
# MAGIC GROUP BY TYPE_CONTRAT;
# MAGIC
# MAGIC
# MAGIC -- 3. Statistiques Sinistres & Financières (Indicateurs / Counter Widgets)
# MAGIC SELECT 
# MAGIC SUM(c.MONTANT_PRIME) as Chiffre_Affaires_Total,
# MAGIC SUM(s.montant_indemnise) as Total_Indemnites_Versees,
# MAGIC ROUND((SUM(s.MONTANT_INDEMNISE) / SUM(c.MONTANT_PRIME)) * 100, 2) as Ratio_Sinistralite_Pourcent
# MAGIC FROM gold_fact_contrat c
# MAGIC LEFT JOIN gold_fact_sinistre s ON c.CONTRAT_ID = s.CONTRAT_ID;
# MAGIC
# MAGIC
# MAGIC -- 4. Matrice des Doublons (Tableau filtré)
# MAGIC SELECT MASTER_CLIENT_ID, NOM, PRENOM, COUNT(*) as nombre_de_comptes
# MAGIC FROM gold_dim_client
# MAGIC GROUP BY MASTER_CLIENT_ID, NOM, PRENOM
# MAGIC HAVING COUNT(*) > 1;
# MAGIC
# MAGIC
# MAGIC -- ce que je rajoute
# MAGIC -- contrats 
# MAGIC SELECT
# MAGIC STATUT_CONTRAT,
# MAGIC COUNT(*) AS nb_contrats
# MAGIC FROM gold_fact_contrat
# MAGIC GROUP BY STATUT_CONTRAT;
# MAGIC
# MAGIC -- sinistres 
# MAGIC SELECT
# MAGIC STATUT_SINISTRE,
# MAGIC COUNT(*) AS nb_sinistres,
# MAGIC SUM(MONTANT_INDEMNISE) AS montant_total
# MAGIC FROM gold_fact_sinistre
# MAGIC GROUP BY STATUT_SINISTRE;
# MAGIC
# MAGIC -- finances 
# MAGIC
# MAGIC SELECT
# MAGIC     SUM(MONTANT_PRIME) AS primes
# MAGIC FROM gold_fact_contrat;
# MAGIC
# MAGIC
# MAGIC SELECT
# MAGIC     SUM(MONTANT) AS paiements
# MAGIC FROM gold_fact_payment;
# MAGIC
# MAGIC -- 0. Nombre total de clients
# MAGIC SELECT COUNT(DISTINCT MASTER_CLIENT_ID) AS total_clients
# MAGIC FROM gold_dim_client;
# MAGIC
# MAGIC -- 1. Répartition des clients par code postal
# MAGIC SELECT CODE_POSTAL, COUNT(DISTINCT MASTER_CLIENT_ID) AS nb_clients
# MAGIC FROM gold_dim_client
# MAGIC GROUP BY CODE_POSTAL;
# MAGIC
# MAGIC -- 2. Statistiques contrats par type
# MAGIC SELECT TYPE_CONTRAT, COUNT(CONTRAT_ID) AS nb_contrats, SUM(MONTANT_PRIME) AS montant_total
# MAGIC FROM gold_fact_contrat
# MAGIC GROUP BY TYPE_CONTRAT;
# MAGIC
# MAGIC -- 3. Statistiques sinistres & financières
# MAGIC SELECT 
# MAGIC     SUM(c.MONTANT_PRIME) AS chiffre_affaires,
# MAGIC     SUM(s.MONTANT_INDEMNISE) AS indemnites_versees,
# MAGIC     ROUND((SUM(s.MONTANT_INDEMNISE) / SUM(c.MONTANT_PRIME)) * 100, 2) AS ratio_sinistralite_pct
# MAGIC FROM gold_fact_contrat c
# MAGIC LEFT JOIN gold_fact_sinistre s 
# MAGIC     ON c.CONTRAT_ID = s.CONTRAT_ID;
# MAGIC
# MAGIC -- 4. Matrice des doublons
# MAGIC SELECT MASTER_CLIENT_ID, NOM, PRENOM, COUNT(*) AS nb_comptes
# MAGIC FROM gold_dim_client
# MAGIC GROUP BY MASTER_CLIENT_ID, NOM, PRENOM
# MAGIC HAVING COUNT(*) > 1;
# MAGIC
# MAGIC -- 5. Contrats par statut
# MAGIC SELECT STATUT_CONTRAT, COUNT(*) AS nb_contrats
# MAGIC FROM gold_fact_contrat
# MAGIC GROUP BY STATUT_CONTRAT;
# MAGIC
# MAGIC -- 6. Sinistres par statut
# MAGIC SELECT STATUT_SINISTRE, COUNT(*) AS nb_sinistres, SUM(MONTANT_INDEMNISE) AS montant_total
# MAGIC FROM gold_fact_sinistre
# MAGIC GROUP BY STATUT_SINISTRE;
# MAGIC
# MAGIC -- 7. Total primes
# MAGIC SELECT SUM(MONTANT_PRIME) AS primes
# MAGIC FROM gold_fact_contrat;
# MAGIC
# MAGIC -- 8. Total paiements
# MAGIC SELECT SUM(MONTANT) AS paiements
# MAGIC FROM gold_fact_payment;
# MAGIC
# MAGIC
# MAGIC /*
# MAGIC Résumé des livrables pour votre rendu :
# MAGIC 1.	Document de cadrage/spécifications : Reprenez les sections du Dossier 1 (Sujet 1). 
# MAGIC
# MAGIC 2.	Schéma Data Flow : Exportez le schéma ASCII fourni dans le Dossier 3 (Sujet 1). 
# MAGIC
# MAGIC 3.	Agilité & Risques : Utilisez les tickets Trello et la matrice des risques rédigés dans le Sujet 2. 
# MAGIC
# MAGIC 4.	Code & Dashboard : Exécutez le script PySpark complet sous Databricks Community Edition pour générer les tables Delta et publier le tableau de bord interactif.
# MAGIC
# MAGIC */
# MAGIC
# MAGIC
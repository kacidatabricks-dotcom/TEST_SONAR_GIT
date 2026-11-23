# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %restart_python
# MAGIC  

# COMMAND ----------

# MAGIC
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

# DBTITLE 1,CREATION DES SCHEMAS
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.gold;

# COMMAND ----------

# DBTITLE 1,Ingestion et Enregistrement en Bronze
from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap, col, lower, upper, trim, concat_ws, levenshtein, when, lit, count, sum as _sum, avg, round, to_date
#from pyspark.sql.types import *  revoir 
#from pyspark.sql.functions import *
import pandas as pd
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, DoubleType
from pyspark.sql import functions as F


# Importer des fichiers des dossiers PIPELINE et TESTS


import sys
import os

#   Récupérer le chemin absolu du dossier courant (NOTEBOOK)
current_dir = os.getcwd()

#  Remonter d'un niveau (dossier parent) avant d'aller dans PIPELINE
#PIPELINE_path = os.path.abspath(os.path.join(current_dir, "..", "PIPELINE"))

PIPELINE_path = current_dir
#  L'ajouter au chemin système
if PIPELINE_path not in sys.path:
    sys.path.append(PIPELINE_path)

print("----PIPELINE path -------------")
print(PIPELINE_path)

#==================================
# Importations des Données oracle
#==================================

# Importer à partir du dossier PIPELINE

import importlib
create_ab_dataframes = importlib.import_module("sonar-fonc-ingestion-des-donnees-oracle").create_ab_dataframes


df_ab_clients, df_ab_contrats, df_ab_sinistres, df_ab_payments = create_ab_dataframes(spark)

# Affichage des Dataframes
print("\n # Affichage des Dataframes, 5 premières Lignes ")

print(df_ab_clients.show(5,truncate = False))
print(df_ab_contrats.show(5,truncate=False))
print(df_ab_sinistres.show(5,truncate=False))
print(df_ab_payments.show(5,truncate=False))


# Protection des données
def protection_bronze(df2, source):
    
    salt = dbutils.secrets.get(scope="PROJET_BIG_DATA_STUDI", key="hash_salt")
    
    if source == "Oracle":
    

     df2 = df2.withColumns({"Email_Hached": F.sha2(F.concat(F.col("AB_EMAIL"), F.lit(salt)), 256),

     "Nom_Hached": F.sha2(F.concat(F.col("AB_NOM"), F.lit(salt)), 256),
     "Prenom_Hached": F.sha2(F.concat(F.col("AB_PRENOM"), F.lit(salt)), 256),
     "Adresse_Hached": F.sha2(F.concat(F.col("AB_ADRESSE"), F.lit(salt)), 256),
     "Telephone_Hached": F.sha2(F.concat(F.col("AB_TELEPHONE"), F.lit(salt)), 256),
     "Num_fiscal_Hached": F.sha2(F.concat(F.col("AB_NUM_FISCAL"), F.lit(salt)), 256),
     "Date_de_naissance_Hached": F.sha2(F.concat(F.col("AB_DATE_NAISSANCE"), F.lit(salt)), 256)}).drop("AB_NOM", "AB_PRENOM","AB_EMAIL","AB_DATE_NAISSANCE","AB_ADRESSE","AB_NUM_FISCAL","AB_TELEPHONE")
    else: 
    
     df2 = df2.withColumns({"Email_Hached": F.sha2(F.concat(F.col("AP_MAIL_ADDRESS"), F.lit(salt)), 256),
     "FULL_NAME_Hached": F.sha2(F.concat(F.col("AP_FULL_NAME"), F.lit(salt)), 256),
     "Nom_Hached": F.sha2(F.concat(F.col("AP_NOM"), F.lit(salt)), 256),
     "Prenom_Hached": F.sha2(F.concat(F.col("AP_PRENOM"), F.lit(salt)), 256),
     "Telephone_Hached": F.sha2(F.concat(F.col("AP_PHONE_NUMBER"), F.lit(salt)), 256),
     "Street_Hached": F.sha2(F.concat(F.col("AP_STREET_ADDRESS"), F.lit(salt)), 256),
     "Date_de_naissance_Hached": F.sha2(F.concat(F.col("AP_BIRTH_DATE"), F.lit(salt)), 256)}).drop("AP_FULL_NAME","AP_MAIL_ADDRESS","AP_BIRTH_DATE","AP_NOM", "AP_PRENOM","AP_PHONE_NUMBER","AP_STREET_ADDRESS")
    return df2

# Protection bronze

df_ab_clients_bronze = protection_bronze(df_ab_clients,"Oracle")


# Affichage des DataFrames après le hachage
print("=== df_ab_clients_protégés ===")
print(df_ab_clients_bronze.show(5, truncate=False))

print("=== df_ab_contrats ===")
print(df_ab_contrats.show(5, truncate=False))

print("=== df_ab_sinistres ===")
print(df_ab_sinistres.show(5, truncate=False))

print("=== df_ab_payments ===")
print(df_ab_payments.show(5, truncate=False))

df_ab_contrats_bronze = df_ab_contrats
df_ab_sinistres_bronze = df_ab_sinistres
df_ab_payments_bronze= df_ab_payments

#     ENREGISTREMENT EN BRONZE   DES DONNEES ORACLE    
enregistrement_bronze = importlib.import_module("sonar-fonc-enregistrement-en-bronze").enregistrement_bronze 

# Tables Oracles
enregistrement_bronze(df_ab_clients_bronze,"workspace.bronze.oracle_clients")
enregistrement_bronze(df_ab_contrats_bronze,"workspace.bronze.oracle_contrats")
enregistrement_bronze(df_ab_sinistres_bronze,"workspace.bronze.oracle_sinistres")
enregistrement_bronze(df_ab_payments_bronze,"workspace.bronze.oracle_payments")


#============================================
# Ingestion des données SQLServer
#============================================

# Import load_ap_data 
load_ap_data = importlib.import_module("sonar-fonc-ingestion-des-donnees-assureplus").load_ap_data

# Obtention des dataframes
df_ap_clients, df_ap_contrats, df_ap_sinistres, df_ap_payments = load_ap_data(spark)

 # Scinder en nom et prénom le full name de Assurplus
def split_full_name(df,col_last_name, col_first_name,col_full_name):

 df = df.withColumn(col_last_name, F.trim(F.split(F.col(col_full_name), " ").getItem(1))) \
             .withColumn(col_first_name, F.trim(F.split(F.col(col_full_name), " ").getItem(0)))
 return df
 # Extraire le nom et prénom à partir du Full_name
df_ap_clients = split_full_name(df_ap_clients,"AP_NOM","AP_PRENOM","AP_FULL_NAME")


# Hachage des données

df_ap_clients_bronze = protection_bronze(df_ap_clients,"SQLServer")



# Affichage df_ap_clients_bronze 
print("=== ASSUREPLUS - Clients ===")
print(df_ap_clients_bronze.show(5, truncate=False))

print("=== ASSUREPLUS - Contrats ===")
print(df_ap_contrats.show(5, truncate=False))

print("=== ASSUREPLUS - Sinistres ===")
print(df_ap_sinistres.show(5, truncate=False))

print("=== ASSUREPLUS - Payments ===")
print(df_ap_payments.show(5, truncate=False))


# ENREGISTREMENT EN BRONZE DES DONNEES SQLServer

df_ap_contrats_bronze = df_ap_contrats
df_ap_sinistres_bronze = df_ap_sinistres
df_ap_payments_bronze= df_ap_payments
      
# Tables SQL Server
enregistrement_bronze(df_ap_clients_bronze,"workspace.bronze.sqlserver_clients")
enregistrement_bronze(df_ap_contrats_bronze,"workspace.bronze.sqlserver_contrats")
enregistrement_bronze(df_ap_payments_bronze,"workspace.bronze.sqlserver_payments")
enregistrement_bronze(df_ap_sinistres_bronze,"workspace.bronze.sqlserver_sinistres")



# COMMAND ----------

# DBTITLE 1,Cleaning et Enregistrement en Silver
#========================================================
#   COUCHE SILVER 
#=====================================================



# Ajout du dossier PIPELINE au sys.path pour pouvoir importer les modules du projet
import sys
import os

PIPELINE_path = os.path.abspath(os.path.join(os.getcwd(), "..", "PIPELINE"))
if PIPELINE_path not in sys.path:
    sys.path.append(PIPELINE_path)

#*****************************************************************************
# SUPPRESSION LIGNES INCOHERENTES : les lignes qui présentent des incohérences
# *****************************************************************************

import importlib
_mod = importlib.import_module("sonar-fonc-suppression-incoherences")
suppression_incoherences_clients = _mod.suppression_incoherences_clients
suppression_incoherences = _mod.suppression_incoherences           



# ====================================================
# SUPPRESSION INCOHERENCES CLIENTS AbAssurance
# ====================================================

# Les controles à effectuer pour détecter les lignes incohérentes
controles_ab_clients = [

    # Identifiant client
    (
        F.col("AB_CLIENT_ID").isNull() |
        (F.col("AB_CLIENT_ID") <= 0),
        "Identifiant client invalide"
    ),

    # Nom
    (
        F.col("AB_NOM").isNull() |
        (F.trim(F.col("AB_NOM")) == ""),
        "Nom client manquant"
    ),

    # Prénom
    (
        F.col("AB_PRENOM").isNull() |
        (F.trim(F.col("AB_PRENOM")) == ""),
        "Prénom client manquant"
    ),

    # Email : validation uniquement si renseignée (accents acceptés dans la partie locale : inès.david1@...)
    (
        F.col("AB_EMAIL").isNotNull() &
        (F.trim(F.col("AB_EMAIL")) != "") &
        (~F.trim(F.col("AB_EMAIL")).rlike(
            r"^[A-Za-z0-9._%+\-À-ÖØ-öø-ÿ]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
        )),
        "Adresse email invalide"
    ),

    # Téléphone
    (
        F.col("AB_TELEPHONE").isNotNull() &
        (F.trim(F.col("AB_TELEPHONE")) != "") &
        (~F.regexp_replace(
            F.col("AB_TELEPHONE"), r"[\s.-]", ""
        ).rlike(r"^(0[1-9]\d{8}|\+33[1-9]\d{8}|0033[1-9]\d{8})$")),
        "Numéro de téléphone invalide"
    ),

    # Code postal
    (
        F.col("AB_CODE_POSTAL").isNotNull() &
        (F.trim(F.col("AB_CODE_POSTAL")) != "") &
        (~F.col("AB_CODE_POSTAL").rlike(r"^\d{5}$")),
        "Code postal invalide"
    ),

    # Numéro fiscal
    (
        F.col("AB_NUM_FISCAL").isNotNull() &
        (F.trim(F.col("AB_NUM_FISCAL")) != "") &
        (~F.col("AB_NUM_FISCAL").rlike(r"^\d{13}$")),
        "Numéro fiscal invalide"
    ),

    # Adresse
    (
        F.col("AB_ADRESSE").isNotNull() &
        (F.trim(F.col("AB_ADRESSE")) == ""),
        "Adresse vide"
    ),

    # Statut client
    (
        F.col("AB_STATUT_CLIENT").isNull() |
        (F.trim(F.col("AB_STATUT_CLIENT")) == ""),
        "Statut client manquant"
    )
]


# Exécution du traitement pour  AB Assurance
df_ab_clients_clean, df_ab_clients_Rejected = suppression_incoherences_clients(
    df_ab_clients,
    controles_ab_clients,
    date_naissance="AB_DATE_NAISSANCE",
    date_creation="AB_DATE_CREATION",
    cle_primaire="AB_CLIENT_ID"
)
# Pour la suite , on ne trvaillera qu'avec des données valides, les enregistrements Rejected seront sauvegardés pour une anlyse ultérieure
df_ab_clients = df_ab_clients_clean

print(" Les 5 premiers  clients valides AbAssurance")
print(df_ab_clients_clean.show(5, truncate = False))
print(" Les 5 premiers clients  Rejected")
print(df_ab_clients_Rejected.show(5, truncate =  False))




# ====================================================
# SUPPRESSION INCOHERENCES USERS AssurePlus
# ====================================================

controles_ap_clients = [

    # Identifiant utilisateur
    (
        F.col("AP_USER_ID").isNull() |
        (F.col("AP_USER_ID") <= 0),
        "Identifiant utilisateur invalide"
    ),

    # Nom complet
    (
        F.col("AP_FULL_NAME").isNull() |
        (F.trim(F.col("AP_FULL_NAME")) == ""),
        "Nom complet manquant"
    ),

    # Email
    (
        F.col("AP_MAIL_ADDRESS").isNotNull() &
        (F.trim(F.col("AP_MAIL_ADDRESS")) != "") &
        (~F.trim(F.col("AP_MAIL_ADDRESS")).rlike(
            r"^[A-Za-z0-9._%+\-À-ÖØ-öø-ÿ]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
        )),
        "Adresse email invalide"
    ),

    # Téléphone
    (
        F.col("AP_PHONE_NUMBER").isNotNull() &
        (F.trim(F.col("AP_PHONE_NUMBER")) != "") &
        (~F.regexp_replace(
            F.col("AP_PHONE_NUMBER"), r"[\s.-]", ""
        ).rlike(r"^(0[1-9]\d{8}|\+33[1-9]\d{8}|0033[1-9]\d{8})$")),
        "Numéro de téléphone invalide"
    ),

    # Code postal
    (
        F.col("AP_ZIP_CODE").isNotNull() &
        (F.trim(F.col("AP_ZIP_CODE")) != "") &
        (~F.col("AP_ZIP_CODE").rlike(r"^\d{5}$")),
        "Code postal invalide"
    ),

    # Adresse
    (
        F.col("AP_STREET_ADDRESS").isNotNull() &
        (F.trim(F.col("AP_STREET_ADDRESS")) == ""),
        "Adresse vide"
    ),

    # Statut client
    (
        F.col("AP_CUSTOMER_STATUS").isNull() |
        (F.trim(F.col("AP_CUSTOMER_STATUS")) == ""),
        "Statut client manquant"
    ),

    # Score fidélité : valeur comprise entre 0 et 100 (try_cast : 'inconnu' -> NULL donc invalide)
    (
        F.col("AP_LOYALTY_SCORE").isNull() |
        F.expr("try_cast(AP_LOYALTY_SCORE as int)").isNull() |
        (F.expr("try_cast(AP_LOYALTY_SCORE as int)") < 0) |
        (F.expr("try_cast(AP_LOYALTY_SCORE as int)") > 100),
        "Score fidélité invalide"
    )
]


# Exécution du traitement les clients AssurePlus
df_ap_clients_clean, df_ap_clients_Rejected = suppression_incoherences_clients(
    df_ap_clients,
    controles_ap_clients,
    date_naissance="AP_BIRTH_DATE",
    date_creation="AP_DATE_CREATION",
    cle_primaire="AP_USER_ID"
)
# Idem, on ne travaillera qu'avec les clients  valides
df_ap_clients = df_ap_clients_clean

print(" Les 5 premiers  clients valides AssurePlus")
print(df_ap_clients_clean.show(5, truncate = False))
print(" Les 5 premiers clients  Rejected")
print(df_ap_clients_Rejected.show(5, truncate =  False))


# ============================================================
#  SUPPRESSION INCOHERENCES  : CONTRATS
# ============================================================

controles_contrats = [

    # Valeurs obligatoires
    (
        F.col("AB_POLICY_NUMBER").isNull() |
        (F.trim(F.col("AB_POLICY_NUMBER")) == ""),
        "Numéro de contrat manquant"
    ),

    (
        F.col("AB_CLIENT_ID").isNull() |
        (F.col("AB_CLIENT_ID") <= 0),
        "Identifiant client invalide"
    ),

    (
        F.col("AB_TYPE_ASSURANCE").isNull() |
        (~F.col("AB_TYPE_ASSURANCE").isin(
            "AUTO", "HABITATION", "SANTE",
            "VIE", "PROFESSIONNELLE", "MOTO"
        )),
        "Type assurance invalide"
    ),

    (
        F.col("AB_STATUT_CONTRAT").isNull() |
        (~F.col("AB_STATUT_CONTRAT").isin(
            "ACTIF", "RESILIE", "SUSPENDU",
            "EN_ATTENTE", "EXPIRE"
        )),
        "Statut contrat invalide"
    ),

    (
        F.col("AB_PRIME_ANNUELLE").isNull() |
        F.isnan(F.col("AB_PRIME_ANNUELLE")) |
        (F.col("AB_PRIME_ANNUELLE") < 0),
        "Prime annuelle invalide"
    ),

    (
        F.col("AB_AGENCE_ID").isNull() |
        (F.col("AB_AGENCE_ID") <= 0),
        "Identifiant agence invalide"
    )
]


# Contrôle de cohérence entre les dates
controles_contrats.append((
    F.col("__date_AB_DATE_FIN") <
    F.col("__date_AB_DATE_DEBUT"),
    "Date fin antérieure à date début"
))


# Dtection des incohérences contrats pour Ab Assurance
df_ab_contrats_clean, df_ab_contrats_Rejected = suppression_incoherences(
    df_ab_contrats,
    controles_contrats.copy(),
    colonnes_dates=["AB_DATE_DEBUT", "AB_DATE_FIN"],
    cle_primaire="AB_POLICY_NUMBER"
)
print(" les 5 premiers contrats valides de  Ab Assurance")
print(df_ab_contrats_clean.show(5))
print(" les contrats Rejected")
print(df_ab_contrats_Rejected.show(5))

# On ne travaillera  qu'avec les enregistrements valides 
df_ab_contrats= df_ab_contrats_clean

# Detection incohérences  spécifiques aux contrats AssurPlus
controles_contrats_ap = [

    # Valeurs obligatoires
    (
        F.col("AP_CONTRACT_REF").isNull() |
        (F.trim(F.col("AP_CONTRACT_REF")) == ""),
        "Numéro de contrat manquant"
    ),

    (
        F.col("AP_USER_ID").isNull() |
        (F.col("AP_USER_ID") <= 0),
        "Identifiant client invalide"
    ),

    (
        F.col("AP_PRODUCT_CODE").isNull() |
        (~F.col("AP_PRODUCT_CODE").isin(
            "Auto", "Habitation", "Santé",
            "Prévoyance"
        )),
        "Type assurance invalide"
    ),

    (
        F.col("AP_CONTRACT_STATE").isNull() |
        (~F.col("AP_CONTRACT_STATE").isin(
            "ACTIF", "RESILIE", "SUSPENDU",
            "EN_ATTENTE", "EXPIRE"
        )),
        "Statut contrat invalide"
    ),

    (
        F.col("AP_MONTHLY_PREMIUM").isNull() |
        F.isnan(F.col("AP_MONTHLY_PREMIUM")) |
        (F.col("AP_MONTHLY_PREMIUM") < 0),
        "Prime invalide"
    ),

    (
        F.col("AP_BROKER_CODE").isNull() |
        (F.col("AP_BROKER_CODE") <= 0),
        "Identifiant courtier invalide"
    ),

    # Contrôle de cohérence entre les dates
    (
        F.col("__date_AP_END_DATE") <
        F.col("__date_AP_START_DATE"),
        "Date fin antérieure à date début"
    )
]

# # Detection des contrats incohérents  pour AssurePlus
df_ap_contrats_clean, df_ap_contrats_Rejected = suppression_incoherences(
    df_ap_contrats,
    controles_contrats_ap,
    colonnes_dates=["AP_START_DATE", "AP_END_DATE"],
    cle_primaire="AP_CONTRACT_REF"
)
print(" les 5 premiers contrats valides AssurePlus")
print(df_ap_contrats_clean.show(5))
print(" les contrats Rejected")
print(df_ap_contrats_Rejected.show(5))
# On ne garde que les enregistrements valides 
df_ap_contrats= df_ap_contrats_clean


# ============================================================
# SUPPRESSION INCOHERENCES : SINISTRES
# ============================================================

controles_sinistres_ab = [

    (
        F.col("AB_CLAIM_ID").isNull() |
        (F.col("AB_CLAIM_ID") <= 0),
        "Identifiant sinistre invalide"
    ),

    (
        F.col("AB_POLICY_NUMBER").isNull() |
        (F.trim(F.col("AB_POLICY_NUMBER")) == ""),
        "Numéro de contrat manquant"
    ),

    (
        F.col("AB_MONTANT_ESTIME").isNull() |
        F.isnan(F.col("AB_MONTANT_ESTIME")) |
        (F.col("AB_MONTANT_ESTIME") < 0),
        "Montant estimé invalide"
    ),

    (
        F.col("AB_STATUT_SINISTRE").isNull() |
        (~F.col("AB_STATUT_SINISTRE").isin(
            "OUVERT", "EN_COURS", "CLOTURE",
            "REFUSE", "EN_EXPERTISE"
        )),
        "Statut sinistre invalide"
    )
]

controles_sinistres_ap = [

    (
        F.col("AP_SINISTRE_NUM").isNull() |
        (F.col("AP_SINISTRE_NUM") <= 0),
        "Identifiant sinistre invalide"
    ),

    (
        F.col("AP_CONTRACT_REF").isNull() |
        (F.trim(F.col("AP_CONTRACT_REF")) == ""),
        "Numéro de contrat manquant"
    ),

    (
        F.col("AP_ESTIMATED_AMOUNT").isNull() |
        F.isnan(F.col("AP_ESTIMATED_AMOUNT")) |
        (F.col("AP_ESTIMATED_AMOUNT") < 0),
        "Montant estimé invalide"
    ),

    (
        F.col("AP_CLAIM_STATUS").isNull() |
        (~F.col("AP_CLAIM_STATUS").isin(
            "OUVERT", "EN_COURS", "CLOTURE",
            "REFUSE", "EN_EXPERTISE"
        )),
        "Statut sinistre invalide"
    )
]



# 
df_ab_sinistres_clean, df_ab_sinistres_Rejected = suppression_incoherences(
    df_ab_sinistres,
    controles_sinistres_ab.copy(),
    colonnes_dates=["AB_DATE_SINISTRE"],
    cle_primaire="AB_CLAIM_ID",
    contrats_valides=df_ab_contrats_clean
)

print(" les sinistres valides AbAssurance")
df_ab_sinistres_clean.show(5, truncate=False)
print(" les sinistres Rejected")
print(df_ab_sinistres_Rejected.show(5,truncate = False))

  # On ne garde que les enregistrements valides 
df_ab_sinistres= df_ab_sinistres_clean



# Dtection des incohérences (sinistres) relatives à AssurePlus
df_ap_sinistres_clean, df_ap_sinistres_Rejected = suppression_incoherences(
    df_ap_sinistres,
    controles_sinistres_ap.copy(),
    colonnes_dates=["AP_INCIDENT_DATE"],
    cle_primaire="AP_SINISTRE_NUM",
    contrats_valides=df_ap_contrats_clean
)

print(" les 5 premiers  sinistres valides AssurePlus")
print(df_ap_sinistres_clean.show(5, truncate = False))
print(" les sinistres Rejected")
print(df_ap_sinistres_Rejected.show(5, truncate =  False))                                                                                                                                                                                           

# On ne garde que les enregistrements valides 
df_ap_sinistres= df_ap_sinistres_clean



# ============================================================
#SUPPRESSION INCOHERENCES : PAYMENTS
# ============================================================

controles_payments_ab = [

    (
        F.col("AB_PAYMENT_ID").isNull() |
        (F.col("AB_PAYMENT_ID") <= 0),
        "Identifiant paiement invalide"
    ),

    (
        F.col("AB_POLICY_NUMBER").isNull() |
        (F.trim(F.col("AB_POLICY_NUMBER")) == ""),
        "Numéro de contrat manquant"
    ),

    (
        F.col("AB_MONTANT").isNull() |
        F.isnan(F.col("AB_MONTANT")) |
        (F.col("AB_MONTANT") <= 0),
        "Montant paiement invalide"
    ),

    (
        F.col("AB_MODE_PAYMENT").isNull() |
        (~F.col("AB_MODE_PAYMENT").isin(
            "CARTE", "PRELEVEMENT", "VIREMENT",
            "CHEQUE", "ESPECES"
        )),
        "Mode de paiement invalide"
    )
]



controles_payments_ap = [

    (
        F.col("AP_PAYMENT_REF").isNull() |
        (F.col("AP_PAYMENT_REF") <= 0),
        "Identifiant paiement invalide"
    ),

    (
        F.col("AP_CONTRACT_REF").isNull() |
        (F.trim(F.col("AP_CONTRACT_REF")) == ""),
        "Numéro de contrat manquant"
    ),

    (
        F.col("AP_AMOUNT_PAID").isNull() |
        F.isnan(F.col("AP_AMOUNT_PAID")) |
        (F.col("AP_AMOUNT_PAID") <= 0),
        "Montant paiement invalide"
    ),

    (
        F.col("AP_PAYMENT_CHANNEL").isNull() |
        (~F.col("AP_PAYMENT_CHANNEL").isin(
            "CARD", "CASH", "BANK_TRANSFER", "DIRECT_DEBIT", "CHEQUE"
        )),
        "Mode de paiement invalide"
    )
]

# Incohérences Payments AbAssurance
df_ab_payments_clean, df_ab_payments_Rejected = suppression_incoherences(
    df_ab_payments,
    controles_payments_ab.copy(),
    colonnes_dates=["AB_DATE_PAYMENT"],
    cle_primaire="AB_PAYMENT_ID",
    contrats_valides=df_ab_contrats_clean
)

print(" Les 5 premiers  payments valides AbAssurance")
print(df_ab_payments_clean.show(5, truncate = False))
print(" Les payments Rejected")
print(df_ab_payments_Rejected.show(5, truncate =  False))

# # On ne garde que les enregistrements valides 
df_ab_payments = df_ab_payments_clean



# Incohérences Payments AssurePlus
df_ap_payments_clean, df_ap_payments_Rejected = suppression_incoherences(
    df_ap_payments,
    controles_payments_ap.copy(),
    colonnes_dates=["AP_PAYMENT_DATETIME"],
    cle_primaire="AP_PAYMENT_REF",
    contrats_valides=df_ap_contrats_clean
)

print(" Les 5 premiers  payments valides AssurePlus")
print(df_ap_payments_clean.show(5, truncate = False))
print(" Les payments Rejected")
print(df_ap_payments_Rejected.show(5, truncate =  False))

# On ne garde que les enregistrements valides 
df_ap_payments = df_ap_payments_clean


# Protection des données pour la couche Silver
def protection_silver(df2, source):
    salt = dbutils.secrets.get(scope="PROJET_BIG_DATA_STUDI", key="hash_salt")
    
    if source == "Oracle":
    

     df2 = df2.withColumns({"Email_Hached": F.sha2(F.concat(F.col("AB_EMAIL"), F.lit(salt)), 256),

     "Nom_Hached": F.sha2(F.concat(F.col("AB_NOM"), F.lit(salt)), 256),
     "Prenom_Hached": F.sha2(F.concat(F.col("AB_PRENOM"), F.lit(salt)), 256),
     "Adresse_Hached": F.sha2(F.concat(F.col("AB_ADRESSE"), F.lit(salt)), 256),
     "Telephone_Hached": F.sha2(F.concat(F.col("AB_TELEPHONE"), F.lit(salt)), 256),
     "Num_fiscal_Hached": F.sha2(F.concat(F.col("AB_NUM_FISCAL"), F.lit(salt)), 256),
     "Date_de_naissance_Hached": F.sha2(F.concat(F.col("AB_DATE_NAISSANCE"), F.lit(salt)), 256)}).drop("AB_NOM", "AB_PRENOM","AB_EMAIL","AB_DATE_NAISSANCE","AB_ADRESSE","AB_NUM_FISCAL","AB_TELEPHONE")
    else: 
    
     df2 = df2.withColumns({"Email_Hached": F.sha2(F.concat(F.col("AP_MAIL_ADDRESS"), F.lit(salt)), 256),
     "FULL_NAME_Hached": F.sha2(F.concat(F.col("AP_FULL_NAME"), F.lit(salt)), 256),
     "Nom_Hached": F.sha2(F.concat(F.col("AP_NOM"), F.lit(salt)), 256),
     "Prenom_Hached": F.sha2(F.concat(F.col("AP_PRENOM"), F.lit(salt)), 256),
     "Telephone_Hached": F.sha2(F.concat(F.col("AP_PHONE_NUMBER"), F.lit(salt)), 256),
     "Street_Hached": F.sha2(F.concat(F.col("AP_STREET_ADDRESS"), F.lit(salt)), 256),
     "Date_de_naissance_Hached": F.sha2(F.concat(F.col("AP_BIRTH_DATE"), F.lit(salt)), 256)}).drop("AP_FULL_NAME","AP_MAIL_ADDRESS","AP_BIRTH_DATE","AP_NOM", "AP_PRENOM","AP_PHONE_NUMBER","AP_STREET_ADDRESS")
    return df2

    


# Ajout des données (SOURCE_SYSTEM, ORIGINAl_ID) pour avoir la trace des sources de provenance (AbAssurance ou AssurePlus)


_mod = importlib.import_module("sonar-fonc-garder-la-trace")
ajoutmétadonnées = _mod.ajoutmétadonnées


# ORACLE
df_ab_clients =ajoutmétadonnées (df_ab_clients,"Oracle","Oracle_clients.csv","AB_CLIENT_ID")
df_ab_contrats =ajoutmétadonnées (df_ab_contrats,"Oracle","Oracle_contrats.csv","AB_POLICY_NUMBER")
df_ab_sinistres =ajoutmétadonnées (df_ab_sinistres,"Oracle","Oracle_sinistres.csv","AB_CLAIM_ID")
df_ab_payments =ajoutmétadonnées (df_ab_payments,"Oracle","Oracle_payments.csv","AB_PAYMENT_ID")


#SQL Server
df_ap_clients =ajoutmétadonnées (df_ap_clients,"SQL","SQL_clients.csv","AP_USER_ID")
df_ap_contrats =ajoutmétadonnées (df_ap_contrats,"SQL","SQL_contrats.csv","AP_CONTRACT_REF")
df_ap_sinistres =ajoutmétadonnées (df_ap_sinistres,"SQL","SQL_sinistres.csv","AP_SINISTRE_NUM")
df_ap_payments =ajoutmétadonnées (df_ap_payments,"SQL","SQL_payments.csv","AP_PAYMENT_REF")


# Nettoyage et normalisation des données des deux bases


_mod = importlib.import_module("sonar-fonc-nettoyage-des-deux-bases")
data_cleaning = _mod.data_cleaning

df_ab_clients, df_ap_clients= data_cleaning(df_ab_clients,df_ap_clients)
 
_mod = importlib.import_module("sonar-fonc-enregistrement-en-silver")
enregistrement_silver = _mod.enregistrement_silver


#************           ENREGISTREMENT EN SILVER      ****************  
#====================================================================

# Hacher les données avant l'enregistrement
df_ab_clients_silver = protection_silver(df_ab_clients,"Oracle")
df_ab_contrats_silver = df_ab_contrats
df_ab_sinistres_silver = df_ab_sinistres
df_ab_payments_silver= df_ab_payments

df_ap_clients_silver = protection_silver(df_ap_clients,"SQLServer")
df_ap_contrats_silver = df_ap_contrats
df_ap_sinistres_silver = df_ap_sinistres
df_ap_payments_silver= df_ap_payments


# Enregistrement des Tables Oracle
enregistrement_silver(df_ab_clients_silver,"workspace.silver.oracle_clients")
enregistrement_silver(df_ab_contrats_silver,"workspace.silver.oracle_contrats")
enregistrement_silver(df_ab_sinistres_silver,"workspace.silver.oracle_sinistres")
enregistrement_silver(df_ab_payments_silver,"workspace.silver.oracle_payments")

# Conservation des élements Rejected de AbAssurance pour une analyse ultérieure

enregistrement_silver(df_ab_clients_Rejected,"workspace.silver.oracle_clients_Rejected")
enregistrement_silver(df_ab_contrats_Rejected,"workspace.silver.oracle_contrats_Rejected")
enregistrement_silver(df_ab_sinistres_Rejected,"workspace.silver.oracle_sinistres_Rejected")
enregistrement_silver(df_ab_payments_Rejected,"workspace.silver.oracle_payments_Rejected")

# Enregistrement des Tables SQL Server
enregistrement_silver(df_ap_clients_silver,"workspace.silver.sqlserver_clients")
enregistrement_silver(df_ap_contrats_silver,"workspace.silver.sqlserver_contrats")
enregistrement_silver(df_ap_payments_silver,"workspace.silver.sqlserver_payments")
enregistrement_silver(df_ap_sinistres_silver,"workspace.silver.sqlserver_sinistres")

# Conservation des élements Rejected de AssurePlus pour une analyse ultérieure

enregistrement_silver(df_ap_clients_Rejected,"workspace.silver.sqlserver_clients_Rejected")
enregistrement_silver(df_ap_contrats_Rejected,"workspace.silver.sqlserver_contrats_Rejected")
enregistrement_silver(df_ap_payments_Rejected,"workspace.silver.sqlserver_payments_Rejected")
enregistrement_silver(df_ap_sinistres_Rejected,"workspace.silver.sqlserver_sinistres_Rejected")




# COMMAND ----------

# DBTITLE 1,MAPPING_UNIFICATION_ET_ENREGISTREMENT GOLD
#===================== COUCHE GOLD ============================#



# Import dynamique : le nom du module contient des tirets -> import classique impossible
import importlib
_mod = importlib.import_module("sonar-fonc-harmonisation-et-unification-clients")
harmonize_clients = _mod.harmonize_clients
#=========================================
# feature/us-05/construire-le-modele-final
#=========================================

# Mapping et unification des clients
dim_clients = harmonize_clients(df_ab_clients,df_ap_clients)
print("dim_clients les 5 premiers clients  finaux")
print(dim_clients.show(5))


#  Matrice de Doublons et Création de la Dimension Client

_mod = importlib.import_module("sonar-fonc-matrice-doublons")
generer_matrice_doublons = _mod.generer_matrice_doublons

# Algorithme d'identification des doublons (Matching exact sur Nom, Prénom, Date de Naissance )
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, upper, initcap


# res_matched, res_dim, res_doublons, res_df = generer_Matrice_Doublons(dim_clients) ok
matched_clients, dim_clients, doublons_df,result_df =generer_matrice_doublons(dim_clients)

print("Matrice des 10 premiers doublons")  
print(dim_clients.join(doublons_df, "MASTER_CLIENT_ID").show(10))

# Protection GOLD

def protection_gold(df):
    
    salt = dbutils.secrets.get(scope="PROJET_BIG_DATA_STUDI", key="hash_salt")
    
    df = df.withColumns({"Email_Hached": F.sha2(F.concat(F.col("EMAIL"), F.lit(salt)), 256),

    "Nom_Hached": F.sha2(F.concat(F.col("NOM"), F.lit(salt)), 256),
    "Prenom_Hached": F.sha2(F.concat(F.col("PRENOM"), F.lit(salt)), 256),
    "Date_de_naissance_Hached": F.sha2(F.concat(F.col("DATE_NAISSANCE"), F.lit(salt)), 256),
    "Numero_fiscal_Hached": F.sha2(F.concat(F.col("NUMERO_FISCAL"), F.lit(salt)), 256),
    "Adresse_Hached": F.sha2(F.concat(F.col("ADRESSE"), F.lit(salt)), 256),
    "Macth_key_Hached" : F.sha2(F.concat(F.col("MATCH_KEY"), F.lit(salt)), 256),
    "TELEPHONE_Hached": F.sha2(F.concat(F.col("TELEPHONE"), F.lit(salt)), 256)}).drop("NOM", "PRENOM","EMAIL","DATE_NAISSANCE", "NUMERO_FISCAL", "ADRESSE", "TELEPHONE","MATCH_KEY")

    return df

print(" ------Matrice des doublons après Hachage---------------")

dim_clients_protégés= protection_gold(dim_clients)
#dim_clients_gold = dim_clients_protege.drop("NOM","DATE_NAISSANCE","PRENOM")
dim_clients_gold = dim_clients_protégés
print(" Les 20 premiers Doublons")
dim_clients_gold.join(doublons_df, "MASTER_CLIENT_ID").show(20, truncate = False)

# Enregistrment GOLD Client

dim_clients_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.dim_clients_gold")



# Traitements  Tables des Faits Contrats,  Sinistres et payments, Couche Gold

_mod = importlib.import_module("sonar-fonc-unification-et-gold-contrat")
unify_and_map_contrats = _mod.unify_and_map_contrats
#======================================
# Unification Contrats
#=====================================
fact_contrats_gold = unify_and_map_contrats(df_ab_contrats, df_ap_contrats, dim_clients_gold)

# feature/us-12/hachage-des-donnees-sensibles 
#===============================================================================================
#======= Masquage des données contrats  avant affichage dans les Dashboard
#===============================================================================================

from pyspark.sql import functions as F

# Remplacer par des astérisques
# Pour les contrats

fact_contrats_gold_Masqued = fact_contrats_gold.withColumn("MASTER_CLIENT_ID_Masqued", F.lit("****")) \
              .withColumn("CONTRAT_ID_Masqued", F.lit("****")).drop("CONTRAT_ID").drop("MASTER_CLIENT_ID")
print("test du masquage")
fact_contrats_gold.show(5, truncate = False)

# Enregistrement en Gold
fact_contrats_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.fact_contrats")

fact_contrats_gold_Masqued.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.fact_contrats_Masqued")

print(" 5 Contrats finaux")
print(fact_contrats_gold.show(5,truncate= False))

# Unification Sinistres

_mod = importlib.import_module("sonar-fonc-unification-et-gold-sinistre")
unify_and_map_sinistres = _mod.unify_and_map_sinistres

fact_sinistres_gold = unify_and_map_sinistres(df_ab_sinistres, df_ap_sinistres, fact_contrats_gold) 

# feature/us-12/hachage-des-donnees-sensibles 
#===============================================================================================
#======= Masquage des données sinistres avant affichage dans les Dashboard
#===============================================================================================

# Pour les sinistres
fact_sinistres_gold_Masqued  = fact_sinistres_gold.withColumn("MASTER_CLIENT_ID_Masqued", F.lit("****")) \
              .withColumn("SINISTRE_ID_Masqued", F.lit("****"))\
              .withColumn("CONTRAT_ID_Masqued", F.lit("****")).drop("MASTER_CLIENT_ID","CONTRAT_ID","SINISTRE_ID") 

#Enregistrement en Gold
fact_sinistres_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.fact_sinistres")

fact_sinistres_gold_Masqued.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.fact_sinistres_Masqued")

print(" 5 sinistres finaux")
print(fact_sinistres_gold.show(5,truncate= False))

# Unification payments

_mod = importlib.import_module("sonar-fonc-unification-et-gold-payment")
unify_and_map_payments = _mod.unify_and_map_payments

fact_payments_gold_mapped = unify_and_map_payments(df_ab_payments, df_ap_payments, fact_contrats_gold)

# feature/us-12/hachage-des-donnees-sensibles 
#===============================================================================================
#======= Masquage des données payments avant affichage dans les Dashboard
#===============================================================================================

fact_payments_gold_mapped_Masqued  = fact_payments_gold_mapped.withColumn("MASTER_CLIENT_ID_Masqued", F.lit("****")) \
              .withColumn("SINISTRE_ID_Masqued", F.lit("****"))\
              .withColumn("CONTRAT_ID_Masqued", F.lit("****")).drop("MASTER_CLIENT_ID","CONTRAT_ID","PAYMENT_ID") 

fact_payments_gold_mapped.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.fact_payments")  

fact_payments_gold_mapped_Masqued.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.fact_payments_Masqued") 

print(" 5 Payments finaux")
print(fact_payments_gold_mapped.show(5,truncate= False))

# 









# COMMAND ----------

# DBTITLE 1,Install coverage
# Installation de coverage (requis par la cellule de tests ci-dessous)
# À relancer au début de chaque nouvelle session de notebook
%pip install coverage

# COMMAND ----------

# DBTITLE 1,PROPOSITIONS DE TESTS
# ============================================================
# STEP 1 : CONFIGURATION ET DÉMARRAGE IMMÉDIAT DU COVERAGE
# ============================================================
import sys
import os
from coverage import Coverage

# Définir le chemin absolu du dossier PIPELINE
PIPELINE_path = "/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/TEST_SONAR_GIT/PIPELINE"

test_path = "/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/TEST_SONAR_GIT/TESTS"

if PIPELINE_path not in sys.path:
    sys.path.append(PIPELINE_path)

if test_path not in sys.path:
   sys.path.append(test_path)

# Dossier des CSV AbAssurance — même valeur que le défaut des modules d'ingestion
# (<racine du dépôt>/DATA/SAMPLE/ABASSURANCE). Les fonctions de test appellent
# create_ab_dataframes(spark, data_dir) avec ce nom, il doit exister dans le namespace.
data_dir = os.path.join(os.path.dirname(PIPELINE_path), "DATA", "SAMPLE", "ABASSURANCE")



# On initialise et on démarre TOUT DE SUITE le coverage, avant tout import !
cov = Coverage(source=[PIPELINE_path])
cov.start()

# ==========================================
# STEP 2 : IMPORTS  DES MODULES 
# ==========================================

import pkgutil
import importlib

print("Importation des modules (PIPELINE) sous surveillance de coverage...")
for module_info in pkgutil.iter_modules([PIPELINE_path]):
    # Les modules PIPELINE portent des noms avec tirets ("sonar-fonc-...") : ce ne sont pas
    # des identifiants Python valides pour "import x", MAIS importlib.import_module
    # les accepte. On ne saute que les fichiers non modules (ex. "__init__.py.py").
    if "." in module_info.name or module_info.name.startswith("_"):
        continue
    # Charge le fichier proprement en tant que module Python valide
    # reload : recharge le module s'il est déjà en mémoire, pour prendre en compte les modifications du fichier
    try:
        module = importlib.import_module(module_info.name)
        module = importlib.reload(module)
    except Exception as e:
        print(f"ATTENTION : import PIPELINE impossible pour {module_info.name} : {e}")
        continue

    # Injecte les fonctions dans l'espace global pour que les  tests unittest les trouvent
    module_attrs = [attr for attr in dir(module) if not attr.startswith('_')]
    for attr in module_attrs:
        globals()[attr] = getattr(module, attr)
# Pour les tests : les fichiers de TESTS sont chargés via exec() (la couverture ne porte que sur PIPELINE).
# exec() fait partager le namespace du notebook, ce qui est indispensable ici car les fonctions
# de test s'appellent entre elles et utilisent les DataFrames créés par les fichiers d'ingestion.
import glob

print("Chargement des fichiers de tests (TESTS)...")
test_files = sorted(glob.glob(os.path.join(test_path, "sonar-test-*.py")))
# Les fichiers d'ingestion en premier : ils créent df_ab_* utilisés par les autres tests
ingestion_files = [f for f in test_files if "ingestion" in os.path.basename(f)]
other_files = [f for f in test_files if f not in ingestion_files]

for f in ingestion_files + other_files:
    try:
        exec(open(f).read())
    except Exception as e:
        print(f"ATTENTION : erreur au chargement de {os.path.basename(f)} : {e}")

# Plusieurs tests utilisent df_ab_* : on les crée via la fonction PIPELINE
# (dans le fichier TESTS d'ingestion Oracle, la création au niveau module est dans un docstring = code mort)
df_ab_clients, df_ab_contrats, df_ab_sinistres, df_ab_payments = create_ab_dataframes(spark)
# Idem pour les DataFrames AssurePlus (df_ap_*), utilisés notamment par harmonize_clients
df_ap_clients, df_ap_contrats, df_ap_sinistres, df_ap_payments = load_ap_data(spark)

# Import des fonctions de tests
import unittest
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# ==========================================
# STEP 3 : EXÉCUTION DE la  CLASSE DE TEST
# ==========================================

# Fonction clean_email utilisée par test_clean_email
def clean_email(value):
    if value is None:
        return None
    return value.strip().lower()


class TestETLPipeline(unittest.TestCase):
    def test_clean_email(self):
        self.assertEqual(clean_email("  JEAN.DUPONT@Email.FR "), "jean.dupont@email.fr")
        self.assertIsNone(clean_email(None))
        
    def test_clean_last_name(self):
        df = df_ab_clients.withColumn("AB_NOM", F.lit("   martin "))
        df_clean = clean_last_name(df, "AB_NOM")
        value = df_clean.select("AB_NOM").collect()[0][0]
        self.assertEqual(value, "MARTIN")
    

    def test_normalize_firstname(self):
        df = df_ab_clients.withColumn("AB_PRENOM", F.lit("  jean "))
        df_norm = df.withColumn("AB_PRENOM", normalize_firstname("AB_PRENOM"))
        value = df_norm.select("AB_PRENOM").collect()[0][0]
        self.assertEqual(value, "JEAN")

    def test_normalize_phone_number(self):
        df = df_ab_clients.withColumn("AB_TELEPHONE", F.lit("0612345678"))
        df_norm = normalize_phone_number(df, "AB_TELEPHONE")
        value = df_norm.select("AB_TELEPHONE").collect()[0][0]
        self.assertTrue(value.startswith("+33"))

    def test_matching_key(self):
        self.assertIn("MATCH_KEY", matched_clients.columns)
        keys = matched_clients.select("MATCH_KEY").distinct().count()
        self.assertTrue(keys > 0)

    def test_master_client_id_uniqueness(self):
        count_id = dim_clients.select("MASTER_CLIENT_ID").distinct().count()
        self.assertTrue(count_id > 0)
    
    def test_gold_fact_contrats(self):   
       #test_unify_and_map_contrats()
        self.assertIsInstance(fact_contrats_gold, DataFrame)
        self.assertTrue(fact_contrats_gold.count() > 0)

    def test_gold_fact_sinistres(self):
        self.assertIsInstance(fact_sinistres_gold, DataFrame)
        self.assertTrue(fact_sinistres_gold.count() > 0)

    def test_doublons(self):
        df = doublons_df
        self.assertTrue("MASTER_CLIENT_ID" in df.columns)
        self.assertTrue(df.count() >= 0)

##

    def test1_create_ab_dataframes(self):
        test_create_ab_dataframes(spark)

    #test_load_ap_data()
    def test1_load_ap_data(self):
        load_ap_data(spark)

    def test1_enregistrement_bronze(self):
        test_enregistrement_bronze()

    def test1_ajoutmétadonnées(self):
        test_ajoutmétadonnées()
# mettre ici le test de nettoyage
    def test1_enregistrement_silver(self):
       test_enregistrement_silver()
# test_harmonize_clients()


    def test1_generer_matrice_doublons(self):
      test_generer_matrice_doublons()

    def test1_save_dim_client_to_gold(self):
      test_save_dim_client_to_gold()
# test_unify_and_map_contrats()  # Test file not found - skipped

     
    def test1_unify_sinistres(self):
      test_unify_sinistres()

    def test1_unify_payments(self):
      test_unify_payments()
    

    def testt1_nettoyage_des_deux_bases(self):
        
        df_ab_meta = ajoutmétadonnées(df_ab_clients, "Oracle", "oracle_clients.csv", "AB_CLIENT_ID")
        df_ap_meta = ajoutmétadonnées(df_ap_clients, "SQL Server", "assureplus_clients.csv", "AP_USER_ID")
        df_unified = harmonize_clients(df_ab_meta, df_ap_meta).withColumn("VILLE", F.lit("Paris"))
        #df = test_nettoyage_des_deux_bases(df_unified)
        df=data_cleaning(df_ab_meta,df_ap_meta)

# ===== =====

    def test1_protection_bronze_oracle(self):
        test_protection_bronze_oracle()

    def test1_protection_bronze_sqlserver(self):
        test_protection_bronze_sqlserver()

    def test1_suppression_incoherences_clients(self):
        test_suppression_incoherences_clients()

    def test1_suppression_incoherences(self):
        test_suppression_incoherences()

    def test1_unify_and_map_contrats(self):
        test_unify_and_map_contrats()

    def test1_unify_and_map_contrats_client_absent(self):
        test_unify_and_map_contrats_client_absent()

    def test1_unify_and_map_contrats_erreur(self):
        test_unify_and_map_contrats_erreur()



# TestLoader().loadTestsFromModule va automatiquement chercher les classes Test
loader = unittest.TestLoader()
suite = loader.loadTestsFromModule(sys.modules[__name__])

# Exécution
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)

# Arrêt et Rapport de couverture
cov.stop()
cov.save()

print("\n ------- RAPPORT DE COUVERTURE ----")
cov.report(ignore_errors=True)
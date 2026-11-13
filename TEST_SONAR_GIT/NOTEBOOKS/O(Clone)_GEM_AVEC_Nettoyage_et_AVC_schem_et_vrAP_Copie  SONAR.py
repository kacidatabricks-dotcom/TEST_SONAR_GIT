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

# DBTITLE 1,INGESTION CLEANING MATCHING MAPPING DOUBLONS
# MAGIC
# MAGIC %restart_python
# MAGIC
# MAGIC
# MAGIC # Étape 1 : Simulation des jeux de données Oracle & SQL Server et enregistrementen bronze
# MAGIC
# MAGIC
# MAGIC from pyspark.sql import SparkSession
# MAGIC from pyspark.sql.functions import initcap, col, lower, upper, trim, concat_ws, levenshtein, when, lit, count, sum as _sum, avg, round, to_date
# MAGIC from pyspark.sql.types import *
# MAGIC from pyspark.sql.functions import *
# MAGIC import pandas as pd
# MAGIC from datetime import datetime
# MAGIC from pyspark.sql.window import Window
# MAGIC
# MAGIC
# MAGIC from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, DoubleType
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC
# MAGIC
# MAGIC # Importer des fichiers des dossiers SRC et TESTS
# MAGIC
# MAGIC
# MAGIC import sys
# MAGIC import os
# MAGIC
# MAGIC #   Récupérer le chemin absolu du dossier courant (NOTEBOOK)
# MAGIC current_dir = os.getcwd()
# MAGIC
# MAGIC #  CORRECTION : Remonter d'un niveau (dossier parent) avant d'aller dans SRC
# MAGIC src_path = os.path.abspath(os.path.join(current_dir, "..", "SRC"))
# MAGIC
# MAGIC
# MAGIC #  L'ajouter au chemin système
# MAGIC if src_path not in sys.path:
# MAGIC     sys.path.append(src_path)
# MAGIC
# MAGIC print("----SRC path -------------")
# MAGIC print(src_path)
# MAGIC
# MAGIC #==================================
# MAGIC # Importations des Données oracle
# MAGIC #==================================
# MAGIC
# MAGIC # 4. Importer à partir du dossier SRC
# MAGIC
# MAGIC from SONAR_FONC_INGESTION_DONNEES_ORACLE import create_ab_dataframes
# MAGIC
# MAGIC
# MAGIC df_ab_clients, df_ab_contrats, df_ab_sinistres, df_ab_payments = create_ab_dataframes(spark)
# MAGIC
# MAGIC # Affichage des Dataframes
# MAGIC print("\n # Affichage des Dataframes, 5 premières Lignes ")
# MAGIC
# MAGIC print(df_ab_clients.show(5))
# MAGIC print(df_ab_contrats.show(5))
# MAGIC print(df_ab_sinistres.show(5))
# MAGIC print(df_ab_payments.show(5))
# MAGIC
# MAGIC
# MAGIC # Protection des données
# MAGIC def protection_bronze(df2, source):
# MAGIC     
# MAGIC     salt = dbutils.secrets.get(scope="PROJET_BIG_DATA_STUDI", key="hash_salt")
# MAGIC     
# MAGIC     if source == "Oracle":
# MAGIC     
# MAGIC
# MAGIC      df2 = df2.withColumns({"Email_Hached": F.sha2(F.concat(F.col("AB_EMAIL"), F.lit(salt)), 256),
# MAGIC
# MAGIC      "Nom_Hached": F.sha2(F.concat(F.col("AB_NOM"), F.lit(salt)), 256),
# MAGIC      "Prenom_Hached": F.sha2(F.concat(F.col("AB_PRENOM"), F.lit(salt)), 256),
# MAGIC      "Date_de_naissance_Hached": F.sha2(F.concat(F.col("AB_DATE_NAISSANCE"), F.lit(salt)), 256)}).drop("AB_NOM", "AB_PRENOM","AB_EMAIL","AB_DATE_NAISSANCE")
# MAGIC     else: 
# MAGIC     
# MAGIC      df2 = df2.withColumns({"Email_Hached": F.sha2(F.concat(F.col("AP_MAIL_ADDRESS"), F.lit(salt)), 256),
# MAGIC      "FULL_NAME_Hached": F.sha2(F.concat(F.col("AP_FULL_NAME"), F.lit(salt)), 256),
# MAGIC      "Nom_Hached": F.sha2(F.concat(F.col("AP_NOM"), F.lit(salt)), 256),
# MAGIC      "Prenom_Hached": F.sha2(F.concat(F.col("AP_PRENOM"), F.lit(salt)), 256),
# MAGIC      "Date_de_naissance_Hached": F.sha2(F.concat(F.col("AP_BIRTH_DATE"), F.lit(salt)), 256)}).drop("AP_FULL_NAME","AP_MAIL_ADDRESS","AP_BIRTH_DATE","AP_NOM", "AP_PRENOM")
# MAGIC     return df2
# MAGIC
# MAGIC # Protection bronze
# MAGIC
# MAGIC df_ab_clients_bronze = protection_bronze(df_ab_clients,"Oracle")
# MAGIC
# MAGIC  #==================================================
# MAGIC # PROTECTION ET VERIFIACTION CONTRAT   SINISTRE PAIEMENT
# MAGIC
# MAGIC # Affichage des DataFrames après le hachage
# MAGIC print("=== df_ab_clients_protege ===")
# MAGIC df_ab_clients_bronze.show(5, truncate=False)
# MAGIC
# MAGIC # Prévoir aussi la protection de ces dataframes
# MAGIC print("=== df_ab_contrats ===")
# MAGIC df_ab_contrats.show(5, truncate=False)
# MAGIC
# MAGIC print("=== df_ab_sinistres ===")
# MAGIC df_ab_sinistres.show(5, truncate=False)
# MAGIC
# MAGIC print("=== df_ab_payments ===")
# MAGIC df_ab_payments.show(5, truncate=False)
# MAGIC
# MAGIC df_ab_contrats_bronze = df_ab_contrats
# MAGIC df_ab_sinistres_bronze = df_ab_sinistres
# MAGIC df_ab_payments_bronze= df_ab_payments
# MAGIC
# MAGIC #     ENREGISTREMENT EN BRONZE   DES DONNEES ORACLE    
# MAGIC from SONAR_FONC_ENREGISTREMENT_EN_BRONZE import enregistrement_bronze 
# MAGIC
# MAGIC # Tables Oracles
# MAGIC enregistrement_bronze(df_ab_clients_bronze,"workspace.bronze.oracle_clients")
# MAGIC enregistrement_bronze(df_ab_contrats_bronze,"workspace.bronze.oracle_contrats")
# MAGIC enregistrement_bronze(df_ab_sinistres_bronze,"workspace.bronze.oracle_sinistres")
# MAGIC enregistrement_bronze(df_ab_payments_bronze,"workspace.bronze.oracle_payments")
# MAGIC
# MAGIC
# MAGIC #============================================
# MAGIC # Ingestion des données SQLServer
# MAGIC #============================================
# MAGIC
# MAGIC # Import load_ap_data 
# MAGIC from SONAR_FONC_INGESTION_DES_DONNEES_ASSUREPLUS import load_ap_data
# MAGIC
# MAGIC # Obtention des dataframes
# MAGIC df_ap_clients, df_ap_contrats, df_ap_sinistres, df_ap_payments = load_ap_data(spark)
# MAGIC
# MAGIC  # Scinder en nom et prénom le full name de Assurplus
# MAGIC def split_full_name(df,col_last_name, col_first_name,col_full_name):
# MAGIC
# MAGIC  df = df.withColumn(col_last_name, F.trim(F.split(F.col(col_full_name), " ").getItem(1))) \
# MAGIC              .withColumn(col_first_name, F.trim(F.split(F.col(col_full_name), " ").getItem(0)))
# MAGIC  return df
# MAGIC  # Extraire le nom et prénom à partir du Full_name
# MAGIC df_ap_clients = split_full_name(df_ap_clients,"AP_NOM","AP_PRENOM","AP_FULL_NAME")
# MAGIC
# MAGIC
# MAGIC # Hachage des données
# MAGIC
# MAGIC df_ap_clients_bronze = protection_bronze(df_ap_clients,"SQLServer")
# MAGIC
# MAGIC
# MAGIC
# MAGIC # Affichage df_ap_clients_bronze 
# MAGIC print("=== ASSUREPLUS - Clients ===")
# MAGIC df_ap_clients_bronze.show(5, truncate=False)
# MAGIC
# MAGIC print("=== ASSUREPLUS - Contrats ===")
# MAGIC df_ap_contrats.show(5, truncate=False)
# MAGIC
# MAGIC print("=== ASSUREPLUS - Sinistres ===")
# MAGIC df_ap_sinistres.show(5, truncate=False)
# MAGIC
# MAGIC print("=== ASSUREPLUS - Payments ===")
# MAGIC df_ap_payments.show(5, truncate=False)
# MAGIC
# MAGIC
# MAGIC # ENREGISTREMENT EN BRONZE DES DONNEES SQLServer
# MAGIC
# MAGIC df_ap_contrats_bronze = df_ap_contrats
# MAGIC df_ap_sinistres_bronze = df_ap_sinistres
# MAGIC df_ap_payments_bronze= df_ap_payments
# MAGIC       
# MAGIC # Tables SQL Server
# MAGIC enregistrement_bronze(df_ap_clients_bronze,"workspace.bronze.sqlserver_clients")
# MAGIC enregistrement_bronze(df_ap_contrats_bronze,"workspace.bronze.sqlserver_contrats")
# MAGIC enregistrement_bronze(df_ap_payments_bronze,"workspace.bronze.sqlserver_payments")
# MAGIC enregistrement_bronze(df_ap_sinistres_bronze,"workspace.bronze.sqlserver_sinistres")
# MAGIC
# MAGIC
# MAGIC #========================================================
# MAGIC #*****   COUCHE SILVER 
# MAGIC #=====================================================
# MAGIC
# MAGIC
# MAGIC #*************************************************
# MAGIC # SUPPRESSION LIGNES INCOHERENTES****************
# MAGIC # *************************************************
# MAGIC
# MAGIC
# MAGIC from SONAR_FONC_SUPPRESSION_INCOHERENCES import suppression_incoherences
# MAGIC
# MAGIC
# MAGIC
# MAGIC # ============================================================
# MAGIC #  SUPPRESSION INCOHERENCES  : CONTRATS
# MAGIC # ============================================================
# MAGIC
# MAGIC controles_contrats = [
# MAGIC
# MAGIC     # Valeurs obligatoires
# MAGIC     (
# MAGIC         F.col("AB_POLICY_NUMBER").isNull() |
# MAGIC         (F.trim(F.col("AB_POLICY_NUMBER")) == ""),
# MAGIC         "Numéro de contrat manquant"
# MAGIC     ),
# MAGIC
# MAGIC     (
# MAGIC         F.col("AB_CLIENT_ID").isNull() |
# MAGIC         (F.col("AB_CLIENT_ID") <= 0),
# MAGIC         "Identifiant client invalide"
# MAGIC     ),
# MAGIC
# MAGIC     (
# MAGIC         F.col("AB_TYPE_ASSURANCE").isNull() |
# MAGIC         (~F.col("AB_TYPE_ASSURANCE").isin(
# MAGIC             "AUTO", "HABITATION", "SANTE",
# MAGIC             "VIE", "PROFESSIONNELLE", "MOTO"
# MAGIC         )),
# MAGIC         "Type assurance invalide"
# MAGIC     ),
# MAGIC
# MAGIC     (
# MAGIC         F.col("AB_STATUT_CONTRAT").isNull() |
# MAGIC         (~F.col("AB_STATUT_CONTRAT").isin(
# MAGIC             "ACTIF", "RESILIE", "SUSPENDU",
# MAGIC             "EN_ATTENTE", "EXPIRE"
# MAGIC         )),
# MAGIC         "Statut contrat invalide"
# MAGIC     ),
# MAGIC
# MAGIC     (
# MAGIC         F.col("AB_PRIME_ANNUELLE").isNull() |
# MAGIC         F.isnan(F.col("AB_PRIME_ANNUELLE")) |
# MAGIC         (F.col("AB_PRIME_ANNUELLE") < 0),
# MAGIC         "Prime annuelle invalide"
# MAGIC     ),
# MAGIC
# MAGIC     (
# MAGIC         F.col("AB_AGENCE_ID").isNull() |
# MAGIC         (F.col("AB_AGENCE_ID") <= 0),
# MAGIC         "Identifiant agence invalide"
# MAGIC     )
# MAGIC ]
# MAGIC
# MAGIC
# MAGIC # Contrôle de cohérence entre les dates
# MAGIC controles_contrats.append((
# MAGIC     F.col("__date_AB_DATE_FIN") <
# MAGIC     F.col("__date_AB_DATE_DEBUT"),
# MAGIC     "Date fin antérieure à date début"
# MAGIC ))
# MAGIC
# MAGIC
# MAGIC # Dtection des incohérences pour Ab Assurance
# MAGIC df_ab_contrats_clean, df_ab_contrats_rejetes = suppression_incoherences(
# MAGIC     df_ab_contrats,
# MAGIC     controles_contrats.copy(),
# MAGIC     colonnes_dates=["AB_DATE_DEBUT", "AB_DATE_FIN"],
# MAGIC     cle_primaire="AB_POLICY_NUMBER"
# MAGIC )
# MAGIC print(" les contrats valides Ab Assurance")
# MAGIC df_ab_contrats_clean.show(5)
# MAGIC print(" les contrats rejetés")
# MAGIC df_ab_contrats_rejetes.show(5)
# MAGIC
# MAGIC # Detection incohérences  spécifiques aux contrats AssurPlus
# MAGIC controles_contrats_ap = [
# MAGIC
# MAGIC     # Valeurs obligatoires
# MAGIC     (
# MAGIC         F.col("AP_CONTRACT_REF").isNull() |
# MAGIC         (F.trim(F.col("AP_CONTRACT_REF")) == ""),
# MAGIC         "Numéro de contrat manquant"
# MAGIC     ),
# MAGIC
# MAGIC     (
# MAGIC         F.col("AP_USER_ID").isNull() |
# MAGIC         (F.col("AP_USER_ID") <= 0),
# MAGIC         "Identifiant client invalide"
# MAGIC     ),
# MAGIC
# MAGIC     (
# MAGIC         F.col("AP_PRODUCT_CODE").isNull() |
# MAGIC         (~F.col("AP_PRODUCT_CODE").isin(
# MAGIC             "Auto", "Habitation", "Santé",
# MAGIC             "Prévoyance"
# MAGIC         )),
# MAGIC         "Type assurance invalide"
# MAGIC     ),
# MAGIC
# MAGIC     (
# MAGIC         F.col("AP_CONTRACT_STATE").isNull() |
# MAGIC         (~F.col("AP_CONTRACT_STATE").isin(
# MAGIC             "ACTIF", "RESILIE", "SUSPENDU",
# MAGIC             "EN_ATTENTE", "EXPIRE"
# MAGIC         )),
# MAGIC         "Statut contrat invalide"
# MAGIC     ),
# MAGIC
# MAGIC     (
# MAGIC         F.col("AP_MONTHLY_PREMIUM").isNull() |
# MAGIC         F.isnan(F.col("AP_MONTHLY_PREMIUM")) |
# MAGIC         (F.col("AP_MONTHLY_PREMIUM") < 0),
# MAGIC         "Prime invalide"
# MAGIC     ),
# MAGIC
# MAGIC     (
# MAGIC         F.col("AP_BROKER_CODE").isNull() |
# MAGIC         (F.col("AP_BROKER_CODE") <= 0),
# MAGIC         "Identifiant courtier invalide"
# MAGIC     ),
# MAGIC
# MAGIC     # Contrôle de cohérence entre les dates
# MAGIC     (
# MAGIC         F.col("__date_AP_END_DATE") <
# MAGIC         F.col("__date_AP_START_DATE"),
# MAGIC         "Date fin antérieure à date début"
# MAGIC     )
# MAGIC ]
# MAGIC
# MAGIC # # Dtection des incohérences pour AssurePlus
# MAGIC df_ap_contrats_clean, df_ap_contrats_rejetes = suppression_incoherences(
# MAGIC     df_ap_contrats,
# MAGIC     controles_contrats_ap,
# MAGIC     colonnes_dates=["AP_START_DATE", "AP_END_DATE"],
# MAGIC     cle_primaire="AP_CONTRACT_REF"
# MAGIC )
# MAGIC print(" les contrats valides AssurePlus")
# MAGIC df_ap_contrats_clean.show(5)
# MAGIC print(" les contrats rejetés")
# MAGIC df_ap_contrats_rejetes.show(5)
# MAGIC
# MAGIC
# MAGIC # ============================================================
# MAGIC # SUPPRESSION INCOHERENCES : SINISTRES
# MAGIC # ============================================================
# MAGIC
# MAGIC controles_sinistres_ab = [
# MAGIC
# MAGIC     (
# MAGIC         F.col("AB_CLAIM_ID").isNull() |
# MAGIC         (F.col("AB_CLAIM_ID") <= 0),
# MAGIC         "Identifiant sinistre invalide"
# MAGIC     ),
# MAGIC
# MAGIC     (
# MAGIC         F.col("AB_POLICY_NUMBER").isNull() |
# MAGIC         (F.trim(F.col("AB_POLICY_NUMBER")) == ""),
# MAGIC         "Numéro de contrat manquant"
# MAGIC     ),
# MAGIC
# MAGIC     (
# MAGIC         F.col("AB_MONTANT_ESTIME").isNull() |
# MAGIC         F.isnan(F.col("AB_MONTANT_ESTIME")) |
# MAGIC         (F.col("AB_MONTANT_ESTIME") < 0),
# MAGIC         "Montant estimé invalide"
# MAGIC     ),
# MAGIC
# MAGIC     (
# MAGIC         F.col("AB_STATUT_SINISTRE").isNull() |
# MAGIC         (~F.col("AB_STATUT_SINISTRE").isin(
# MAGIC             "OUVERT", "EN_COURS", "CLOTURE",
# MAGIC             "REFUSE", "EN_EXPERTISE"
# MAGIC         )),
# MAGIC         "Statut sinistre invalide"
# MAGIC     )
# MAGIC ]
# MAGIC
# MAGIC controles_sinistres_ap = [
# MAGIC
# MAGIC     (
# MAGIC         F.col("AP_SINISTRE_NUM").isNull() |
# MAGIC         (F.col("AP_SINISTRE_NUM") <= 0),
# MAGIC         "Identifiant sinistre invalide"
# MAGIC     ),
# MAGIC
# MAGIC     (
# MAGIC         F.col("AP_CONTRACT_REF").isNull() |
# MAGIC         (F.trim(F.col("AP_CONTRACT_REF")) == ""),
# MAGIC         "Numéro de contrat manquant"
# MAGIC     ),
# MAGIC
# MAGIC     (
# MAGIC         F.col("AP_ESTIMATED_AMOUNT").isNull() |
# MAGIC         F.isnan(F.col("AP_ESTIMATED_AMOUNT")) |
# MAGIC         (F.col("AP_ESTIMATED_AMOUNT") < 0),
# MAGIC         "Montant estimé invalide"
# MAGIC     ),
# MAGIC
# MAGIC     (
# MAGIC         F.col("AP_CLAIM_STATUS").isNull() |
# MAGIC         (~F.col("AP_CLAIM_STATUS").isin(
# MAGIC             "OUVERT", "EN_COURS", "CLOTURE",
# MAGIC             "REFUSE", "EN_EXPERTISE"
# MAGIC         )),
# MAGIC         "Statut sinistre invalide"
# MAGIC     )
# MAGIC ]
# MAGIC
# MAGIC
# MAGIC
# MAGIC # 
# MAGIC df_ab_sinistres_clean, df_ab_sinistres_rejetes = suppression_incoherences(
# MAGIC     df_ab_sinistres,
# MAGIC     controles_sinistres_ab.copy(),
# MAGIC     colonnes_dates=["AB_DATE_SINISTRE"],
# MAGIC     cle_primaire="AB_CLAIM_ID",
# MAGIC     contrats_valides=df_ab_contrats_clean
# MAGIC )
# MAGIC
# MAGIC print(" les sinistres valides AbAssurance")
# MAGIC df_ab_sinistres_clean.show(5)
# MAGIC print(" les sinistres rejetés")
# MAGIC df_ab_sinistres_rejetes.show(5)
# MAGIC
# MAGIC # Dtecetion des incohérences (sinistres) relatives à AssurePlus
# MAGIC df_ap_sinistres_clean, df_ap_sinistres_rejetes = suppression_incoherences(
# MAGIC     df_ap_sinistres,
# MAGIC     controles_sinistres_ap.copy(),
# MAGIC     colonnes_dates=["AP_INCIDENT_DATE"],
# MAGIC     cle_primaire="AP_SINISTRE_NUM",
# MAGIC     contrats_valides=df_ap_contrats_clean
# MAGIC )
# MAGIC
# MAGIC print(" les sinistres valides AssurePlus")
# MAGIC df_ap_sinistres_clean.show(5)
# MAGIC print(" les sinistres rejetés")
# MAGIC df_ap_sinistres_rejetes.show(5)
# MAGIC
# MAGIC
# MAGIC
# MAGIC # ============================================================
# MAGIC #SUPPRESSION INCOHERENCES : PAYMENTS
# MAGIC # ============================================================
# MAGIC
# MAGIC controles_payments_ab = [
# MAGIC
# MAGIC     (
# MAGIC         F.col("AB_PAYMENT_ID").isNull() |
# MAGIC         (F.col("AB_PAYMENT_ID") <= 0),
# MAGIC         "Identifiant paiement invalide"
# MAGIC     ),
# MAGIC
# MAGIC     (
# MAGIC         F.col("AB_POLICY_NUMBER").isNull() |
# MAGIC         (F.trim(F.col("AB_POLICY_NUMBER")) == ""),
# MAGIC         "Numéro de contrat manquant"
# MAGIC     ),
# MAGIC
# MAGIC     (
# MAGIC         F.col("AB_MONTANT").isNull() |
# MAGIC         F.isnan(F.col("AB_MONTANT")) |
# MAGIC         (F.col("AB_MONTANT") <= 0),
# MAGIC         "Montant paiement invalide"
# MAGIC     ),
# MAGIC
# MAGIC     (
# MAGIC         F.col("AB_MODE_PAYMENT").isNull() |
# MAGIC         (~F.col("AB_MODE_PAYMENT").isin(
# MAGIC             "CARTE", "PRELEVEMENT", "VIREMENT",
# MAGIC             "CHEQUE", "ESPECES"
# MAGIC         )),
# MAGIC         "Mode de paiement invalide"
# MAGIC     )
# MAGIC ]
# MAGIC
# MAGIC
# MAGIC
# MAGIC controles_payments_ap = [
# MAGIC
# MAGIC     (
# MAGIC         F.col("AP_PAYMENT_REF").isNull() |
# MAGIC         (F.col("AP_PAYMENT_REF") <= 0),
# MAGIC         "Identifiant paiement invalide"
# MAGIC     ),
# MAGIC
# MAGIC     (
# MAGIC         F.col("AP_CONTRACT_REF").isNull() |
# MAGIC         (F.trim(F.col("AP_CONTRACT_REF")) == ""),
# MAGIC         "Numéro de contrat manquant"
# MAGIC     ),
# MAGIC
# MAGIC     (
# MAGIC         F.col("AP_AMOUNT_PAID").isNull() |
# MAGIC         F.isnan(F.col("AP_AMOUNT_PAID")) |
# MAGIC         (F.col("AP_AMOUNT_PAID") <= 0),
# MAGIC         "Montant paiement invalide"
# MAGIC     ),
# MAGIC
# MAGIC     (
# MAGIC         F.col("AP_PAYMENT_CHANNEL").isNull() |
# MAGIC         (~F.col("AP_PAYMENT_CHANNEL").isin(
# MAGIC             "Carte bancaire", "Prélèvement", "Virement",
# MAGIC             "Chèque"
# MAGIC         )),
# MAGIC         "Mode de paiement invalide"
# MAGIC     )
# MAGIC ]
# MAGIC
# MAGIC # Incohérences Payments AbAssurance
# MAGIC df_ab_payments_clean, df_ab_payments_rejetes = suppression_incoherences(
# MAGIC     df_ab_payments,
# MAGIC     controles_payments_ab.copy(),
# MAGIC     colonnes_dates=["AB_DATE_PAYMENT"],
# MAGIC     cle_primaire="AB_PAYMENT_ID",
# MAGIC     contrats_valides=df_ab_contrats_clean
# MAGIC )
# MAGIC
# MAGIC
# MAGIC # Exécution sur AP
# MAGIC df_ap_payments_clean, df_ap_payments_rejetes = suppression_incoherences(
# MAGIC     df_ap_payments,
# MAGIC     controles_payments_ap.copy(),
# MAGIC     colonnes_dates=["AP_PAYMENT_DATETIME"],
# MAGIC     cle_primaire="AP_PAYMENT_REF",
# MAGIC     contrats_valides=df_ap_contrats_clean
# MAGIC )
# MAGIC
# MAGIC # aprés suppression des lignes invalides 
# MAGIC df_ab_contrats = df_ab_contrats_clean
# MAGIC df_ab_sinistres = df_ab_sinistres_clean
# MAGIC df_ab_payments = df_ab_payments_clean
# MAGIC
# MAGIC df_ap_contrats = df_ap_contrats_clean
# MAGIC df_ap_sinistres = df_ap_sinistres_clean
# MAGIC df_ap_payments = df_ap_payments_clean
# MAGIC
# MAGIC
# MAGIC
# MAGIC from SONAR_FONC_ENREGISTREMENT_EN_SILVER import enregistrement_silver
# MAGIC
# MAGIC # Protection des données por la couche Silver
# MAGIC def protection_silver(df2, source):
# MAGIC     
# MAGIC
# MAGIC     salt = dbutils.secrets.get(scope="PROJET_BIG_DATA_STUDI", key="hash_salt")
# MAGIC     
# MAGIC     if source == "Oracle":
# MAGIC     
# MAGIC
# MAGIC      df2 = df2.withColumns({"Email_Hached": F.sha2(F.concat(F.col("AB_EMAIL"), F.lit(salt)), 256),
# MAGIC
# MAGIC      "Nom_Hached": F.sha2(F.concat(F.col("AB_NOM"), F.lit(salt)), 256),
# MAGIC      "Prenom_Hached": F.sha2(F.concat(F.col("AB_PRENOM"), F.lit(salt)), 256),
# MAGIC      "Date_de_naissance_Hached": F.sha2(F.concat(F.col("AB_DATE_NAISSANCE"), F.lit(salt)), 256)}).drop("AB_NOM", "AB_PRENOM","AB_EMAIL","AB_DATE_NAISSANCE")
# MAGIC      return df2
# MAGIC     else: 
# MAGIC     
# MAGIC      df2 = df2.withColumns({"Email_Hached": F.sha2(F.concat(F.col("AP_MAIL_ADDRESS"), F.lit(salt)), 256),
# MAGIC      "FULL_NAME_Hached": F.sha2(F.concat(F.col("AP_FULL_NAME"), F.lit(salt)), 256),
# MAGIC      "Nom_Hached": F.sha2(F.concat(F.col("AP_NOM"), F.lit(salt)), 256),
# MAGIC      "Prenom_Hached": F.sha2(F.concat(F.col("AP_PRENOM"), F.lit(salt)), 256),
# MAGIC      "Date_de_naissance_Hached": F.sha2(F.concat(F.col("AP_BIRTH_DATE"), F.lit(salt)), 256)}).drop("AP_FULL_NAME","AP_MAIL_ADDRESS","AP_BIRTH_DATE","AP_NOM", "AP_PRENOM")
# MAGIC      return df2
# MAGIC
# MAGIC
# MAGIC # Ajout des données pour avoir la trace
# MAGIC
# MAGIC
# MAGIC from SONAR_FONC_GARDER_LA_TRACE import ajoutmétadonnées
# MAGIC
# MAGIC
# MAGIC # ORACLE
# MAGIC df_ab_clients =ajoutmétadonnées (df_ab_clients,"Oracle","Oracle_clients.csv","AB_CLIENT_ID")
# MAGIC df_ab_contrats =ajoutmétadonnées (df_ab_contrats,"Oracle","Oracle_contrats.csv","AB_POLICY_NUMBER")
# MAGIC df_ab_sinistres =ajoutmétadonnées (df_ab_sinistres,"Oracle","Oracle_sinistres.csv","AB_CLAIM_ID")
# MAGIC df_ab_payments =ajoutmétadonnées (df_ab_payments,"Oracle","Oracle_payments.csv","AB_PAYMENT_ID")
# MAGIC
# MAGIC
# MAGIC #SQL Server
# MAGIC df_ap_clients =ajoutmétadonnées (df_ap_clients,"SQL","SQL_clients.csv","AP_USER_ID")
# MAGIC df_ap_contrats =ajoutmétadonnées (df_ap_contrats,"Oracle","Oracle_contrats.csv","AP_CONTRACT_REF")
# MAGIC df_ap_sinistres =ajoutmétadonnées (df_ap_sinistres,"Oracle","Oracle_sinistres.csv","AP_SINISTRE_NUM")
# MAGIC df_ap_payments =ajoutmétadonnées (df_ap_payments,"Oracle","Oracle_payments.csv","AP_PAYMENT_REF")
# MAGIC
# MAGIC
# MAGIC df_ab_clients.show(10, truncate=False)
# MAGIC df_ap_clients.show(10, truncate=False)
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC # Étape 2 : Data Pipeline - Harmonisation & Unification (Silver Layer)
# MAGIC
# MAGIC """
# MAGIC
# MAGIC Schéma client final :
# MAGIC
# MAGIC MASTER_CLIENT_ID
# MAGIC SOURCE_SYSTEM
# MAGIC SOURCE_CLIENT_ID
# MAGIC NOM
# MAGIC PRENOM
# MAGIC NOM_COMPLET
# MAGIC DATE_NAISSANCE
# MAGIC EMAIL
# MAGIC TELEPHONE
# MAGIC ADRESSE
# MAGIC CODE_POSTAL
# MAGIC NUMERO_FISCAL
# MAGIC DATE_CREATION
# MAGIC STATUT_CLIENT
# MAGIC SCORE_FIDELITE
# MAGIC DATE_INTEGRATION
# MAGIC SOURCE_FILE
# MAGIC INGESTION_TIMESTAMP
# MAGIC VILLE     je ne l'ai pas rajouté
# MAGIC
# MAGIC
# MAGIC """
# MAGIC
# MAGIC # Nettoyage des données
# MAGIC #========================  AJOUTER            CONTRAT SINISTRE ET PAYMENTS
# MAGIC
# MAGIC from SONAR_FONC_NETTOYAGE_DES_DEUX_BASES import data_cleaning
# MAGIC
# MAGIC df_ab_clients, df_ap_clients= data_cleaning(df_ab_clients,df_ap_clients)
# MAGIC  
# MAGIC from SONAR_FONC_ENREGISTREMENT_EN_SILVER import enregistrement_silver
# MAGIC
# MAGIC
# MAGIC #                                   ENREGISTREMENT EN SILVER        
# MAGIC # ATTENTION: Toutes les tables ne sont pas enregistrées dans la base de données 
# MAGIC
# MAGIC
# MAGIC # Hacher les données avant l'enregistrement
# MAGIC df_ab_clients_silver = protection_silver(df_ab_clients,"Oracle")
# MAGIC  # à faire drop 
# MAGIC df_ab_contrats_silver = df_ab_contrats
# MAGIC df_ab_sinistres_silver = df_ab_sinistres
# MAGIC df_ab_payments_silver= df_ab_payments
# MAGIC
# MAGIC df_ap_clients_silver = protection_silver(df_ap_clients,"SQLServer")
# MAGIC # à faire drop 
# MAGIC df_ap_contrats_silver = df_ap_contrats
# MAGIC df_ap_sinistres_silver = df_ap_sinistres
# MAGIC df_ap_payments_silver= df_ap_payments
# MAGIC
# MAGIC
# MAGIC # Tables Oracle
# MAGIC enregistrement_silver(df_ab_clients_silver,"workspace.silver.oracle_clients")
# MAGIC enregistrement_silver(df_ab_contrats_silver,"workspace.silver.oracle_contrats")
# MAGIC enregistrement_silver(df_ab_sinistres_silver,"workspace.silver.oracle_sinistres")
# MAGIC # test 
# MAGIC enregistrement_silver(df_ab_payments_rejetes,"workspace.silver.oracle_payments_rejetes")
# MAGIC
# MAGIC enregistrement_silver(df_ab_payments_silver,"workspace.silver.oracle_payments")
# MAGIC
# MAGIC # Tables SQL Server
# MAGIC enregistrement_silver(df_ap_clients_silver,"workspace.silver.sqlserver_clients")
# MAGIC enregistrement_silver(df_ap_contrats_silver,"workspace.silver.sqlserver_contrats")
# MAGIC enregistrement_silver(df_ap_payments_silver,"workspace.silver.sqlserver_payments")
# MAGIC enregistrement_silver(df_ap_sinistres_silver,"workspace.silver.sqlserver_sinistres")
# MAGIC
# MAGIC
# MAGIC #####   ICI NORMALEMENT COMMENCE LA COUCHE GOLD ###################################
# MAGIC
# MAGIC
# MAGIC
# MAGIC from SONAR_FONC_HARMONISATION_ET_UNIFICATION_CLIENTS import harmonize_clients
# MAGIC
# MAGIC
# MAGIC dim_clients = harmonize_clients(df_ab_clients,df_ap_clients)
# MAGIC print("dim_clients")
# MAGIC dim_clients.show(truncate = False)
# MAGIC
# MAGIC
# MAGIC # Étape 3 : Matrice de Doublons et Création de la Dimension Client
# MAGIC
# MAGIC from SONAR_FONC_MATRICE_DOUBLONS import generer_matrice_doublons
# MAGIC
# MAGIC # Algorithme d'identification des doublons (Matching exact sur Nom, Prénom, Date de Naissance ou Téléphone)
# MAGIC from pyspark.sql.window import Window
# MAGIC from pyspark.sql.functions import dense_rank, upper, initcap
# MAGIC
# MAGIC
# MAGIC # Hypothèse : df_clients_initial est votre DataFrame propre d'origine
# MAGIC # Si vous n'avez que dim_clients, assurez-vous de réexécuter la cellule du dessus qui le génère initialement.
# MAGIC
# MAGIC # res_matched, res_dim, res_doublons, res_df = generer_Matrice_Doublons(dim_clients)     ceci marche 
# MAGIC matched_clients, dim_clients, doublons_df,result_df =generer_matrice_doublons(dim_clients)
# MAGIC
# MAGIC print("Matrice des doublons")
# MAGIC display(dim_clients.join(doublons_df, "MASTER_CLIENT_ID"))
# MAGIC
# MAGIC # Protection GOLD
# MAGIC
# MAGIC def protection_gold(df):
# MAGIC     
# MAGIC     salt = dbutils.secrets.get(scope="PROJET_BIG_DATA_STUDI", key="hash_salt")
# MAGIC     
# MAGIC     df = df.withColumns({"Email_Hached": F.sha2(F.concat(F.col("EMAIL"), F.lit(salt)), 256),
# MAGIC
# MAGIC     "Nom_Hached": F.sha2(F.concat(F.col("NOM"), F.lit(salt)), 256),
# MAGIC     "Prenom_Hached": F.sha2(F.concat(F.col("PRENOM"), F.lit(salt)), 256),
# MAGIC     "Date_de_naissance_Hached": F.sha2(F.concat(F.col("DATE_NAISSANCE"), F.lit(salt)), 256),
# MAGIC     "Numero_fiscal_Hached": F.sha2(F.concat(F.col("NUMERO_FISCAL"), F.lit(salt)), 256),
# MAGIC     "Adresse_Hached": F.sha2(F.concat(F.col("ADRESSE"), F.lit(salt)), 256),
# MAGIC     "TELEPHONE_Hached": F.sha2(F.concat(F.col("TELEPHONE"), F.lit(salt)), 256)}).drop("NOM", "PRENOM","EMAIL","DATE_NAISSANCE", "NUMERO_FISCAL", "ADRESSE", "TELEPHONE")
# MAGIC
# MAGIC     return df
# MAGIC
# MAGIC print(" ------Matrice des doublons après Hachage---------------")
# MAGIC
# MAGIC dim_clients_protege= protection_gold(dim_clients)
# MAGIC #dim_clients_gold = dim_clients_protege.drop("NOM","DATE_NAISSANCE","PRENOM")
# MAGIC dim_clients_gold = dim_clients_protege
# MAGIC display(dim_clients_gold.join(doublons_df, "MASTER_CLIENT_ID"))
# MAGIC
# MAGIC # Enregistrment GOLD Client
# MAGIC
# MAGIC
# MAGIC dim_clients_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.dim_clients_gold")
# MAGIC
# MAGIC
# MAGIC # Traitements  Tables de Faits Contrats et Sinistres et payments , Couche Gold
# MAGIC
# MAGIC from SONAR_FONC_UNIFICATION_ET_GOLD_CONTRAT import unify_and_map_contrats
# MAGIC
# MAGIC # Unification Contrats
# MAGIC
# MAGIC fact_contrats_gold = unify_and_map_contrats(df_ab_contrats, df_ap_contrats, dim_clients)
# MAGIC
# MAGIC
# MAGIC fact_contrats_gold.show(truncate=False)
# MAGIC # faire une protection gold 
# MAGIC
# MAGIC fact_contrats_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.fact_contrats")   # changer le nom de la table?
# MAGIC
# MAGIC
# MAGIC
# MAGIC # Unification Sinistres
# MAGIC
# MAGIC from  SONAR_FONC_UNIFICATION_ET_GOLD_SINISTRE   import unify_and_map_sinistres
# MAGIC
# MAGIC fact_sinistres_gold = unify_and_map_sinistres(df_ab_sinistres, df_ap_sinistres, fact_contrats_gold, table_name="workspace.gold.fact_sinistres")   # changer le nom de la table
# MAGIC
# MAGIC
# MAGIC
# MAGIC # Unification payments
# MAGIC
# MAGIC from  SONAR_FONC_UNIFICATION_ET_GOLD_PAYMENT   import unify_and_map_payments
# MAGIC
# MAGIC fact_payments_gold_mapped = unify_and_map_payments(df_ab_payments, df_ap_payments, fact_contrats_gold, table_name="workspace.gold.fact_payments") # changer le nom de la table
# MAGIC
# MAGIC
# MAGIC
# MAGIC #Dossier 3 (Suite) : SONAR_TESTs unitaires, Qualité & Dashboards
# MAGIC
# MAGIC #Question 1 & 2 : SONAR_TESTs unitaires PySpark & SONAR_TEST Coverage
# MAGIC # Pour valider votre code (compatible avec les SONAR_TESTs unitaires PySpark) :
# MAGIC
# MAGIC # voir cellule 3
# MAGIC
# MAGIC
# MAGIC # Question 3 : Identification d'un bug potentiel dans le pipeline
# MAGIC """
# MAGIC •	Bug identifié : Clés primaires dupliquées après l'UNION simple.
# MAGIC
# MAGIC •	Explication : Si AbAssurance et AssurePlus utilisent tous deux des identifiants numériques séquentiels (ID_CLIENT = 101 dans Oracle et CustomerID = 101 dans SQL Server), faire une simple jointure entraîne de fausses associations.
# MAGIC
# MAGIC •	Correction : Toujours créer une clé composite (source_system + original_id) ou générer une clé surrogate unique via UUID / dense_rank() comme réalisé dans l'étape 3.
# MAGIC
# MAGIC """
# MAGIC
# MAGIC """
# MAGIC # Création du Dashboard directement sous Databricks
# MAGIC # Pour réaliser les visualisations demandées : 
# MAGIC
# MAGIC
# MAGIC #1.	Créez une nouvelle cellule SQL dans votre notebook.
# MAGIC
# MAGIC #2.	Exécutez les 4 requêtes ci-dessous.
# MAGIC
# MAGIC #3.	Cliquez sur l'icône "+" à côté des résultats de la cellule -> Visualization, sélectionnez le type de graphique, puis cliquez sur Add to Dashboard. 
# MAGIC """
# MAGIC """
# MAGIC #SQL
# MAGIC #-- 1. Répartition des clients par Ville (Graphique Camembert / Pie Chart)
# MAGIC SELECT ville, COUNT(DISTINCT master_client_id) as total_clients
# MAGIC FROM gold_dim_client
# MAGIC GROUP BY ville;
# MAGIC
# MAGIC #-- 2. Statistiques Contrats (Graphique en Barres)
# MAGIC SELECT type_contrat, COUNT(contrat_id) as nb_contrats, SUM(montant_prime) as total_primes
# MAGIC FROM gold_fact_contrat
# MAGIC GROUP BY type_contrat;
# MAGIC
# MAGIC #-- 3. Statistiques Sinistres & Financières (Indicateurs / Counter Widgets)
# MAGIC SELECT 
# MAGIC SUM(c.montant_prime) as Chiffre_Affaires_Total,
# MAGIC SUM(s.montant_indemnise) as Total_Indemnites_Versees,
# MAGIC ROUND((SUM(s.montant_indemnise) / SUM(c.montant_prime)) * 100, 2) as Ratio_Sinistralite_Pourcent
# MAGIC FROM gold_fact_contrat c
# MAGIC LEFT JOIN gold_fact_sinistres ON c.contrat_id = s.contrat_id;
# MAGIC
# MAGIC #-- 4. Matrice des Doublons (Tableau filtré)
# MAGIC SELECT master_client_id, nom, prenom, COUNT(*) as nombre_de_comptes
# MAGIC FROM gold_dim_client
# MAGIC GROUP BY master_client_id, nom, prenom
# MAGIC HAVING COUNT(*) > 1;
# MAGIC """
# MAGIC """
# MAGIC Résumé des livrables pour votre rendu :
# MAGIC  
# MAGIC
# MAGIC 4.	Code & Dashboard : Exécutez le script PySpark complet sous Databricks Community Edition pour générer les tables Delta et publier le tableau de bord interactif.
# MAGIC
# MAGIC """
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,CELL POUR dashboard bloc 333 les dernieres lignes
# MAGIC %sql
# MAGIC -- CREATE DASHBOARD: dashboard bloc 333
# MAGIC -- Les requêtes suivantes peuvent être ajoutées comme widgets dans le dashboard "dashboard bloc 333"
# MAGIC
# MAGIC -- 1. Nombre total de clients
# MAGIC SELECT COUNT(DISTINCT MASTER_CLIENT_ID) AS total_clients
# MAGIC FROM gold_dim_clients;
# MAGIC
# MAGIC -- 2. Répartition des clients par code postal
# MAGIC SELECT CODE_POSTAL, COUNT(DISTINCT MASTER_CLIENT_ID) AS nb_clients
# MAGIC FROM gold_dim_clients
# MAGIC GROUP BY CODE_POSTAL;
# MAGIC
# MAGIC -- 3. Statistiques contrats par type
# MAGIC SELECT TYPE_CONTRAT, COUNT(CONTRAT_ID) AS nb_contrats, SUM(MONTANT_PRIME) AS montant_total
# MAGIC FROM gold_fact_contrat
# MAGIC GROUP BY TYPE_CONTRAT;
# MAGIC
# MAGIC -- 4. Statistiques sinistres & financières
# MAGIC SELECT 
# MAGIC     SUM(c.MONTANT_PRIME) AS chiffre_affaires,
# MAGIC     SUM(s.MONTANT_INDEMNISE) AS indemnites_versees,
# MAGIC     ROUND((SUM(s.MONTANT_INDEMNISE) / SUM(c.MONTANT_PRIME)) * 100, 2) AS ratio_sinistralite_pct
# MAGIC FROM gold_fact_contrat c
# MAGIC LEFT JOIN gold_fact_sinistres 
# MAGIC     ON c.CONTRAT_ID = s.CONTRAT_ID;
# MAGIC
# MAGIC -- 5. Matrice des doublons
# MAGIC SELECT MASTER_CLIENT_ID, Nom_Hached, Prenom_Hached, COUNT(*) AS nb_comptes
# MAGIC FROM gold_dim_clients
# MAGIC GROUP BY MASTER_CLIENT_ID, Nom_Hached, Prenom_Hached
# MAGIC HAVING COUNT(*) > 1;
# MAGIC --SQL
# MAGIC -- 1. Répartition des clients par Ville (Graphique Camembert / Pie Chart)
# MAGIC --69
# MAGIC
# MAGIC SELECT CODE_POSTAL, COUNT(DISTINCT master_client_id) as total_clients
# MAGIC FROM gold_dim_clients
# MAGIC GROUP BY CODE_POSTAL;
# MAGIC -- 2. Statistiques Contrats (Graphique en Barres)
# MAGIC
# MAGIC SELECT type_contrat, COUNT(contrat_id) as nb_contrats, SUM(montant_prime) as total_primes
# MAGIC FROM gold_fact_contrat
# MAGIC GROUP BY type_contrat;
# MAGIC
# MAGIC -- 3. Statistiques Sinistres & Financières (Indicateurs / Counter Widgets)
# MAGIC
# MAGIC SELECT
# MAGIC SUM(c.montant_prime) as Chiffre_Affaires_Total,
# MAGIC SUM(s.montant_indemnise) as Total_Indemnites_Versees,
# MAGIC ROUND((SUM(s.montant_indemnise) / SUM(c.montant_prime)) * 100, 2) as Ratio_Sinistralite_Pourcent
# MAGIC FROM gold_fact_contrat c
# MAGIC LEFT JOIN gold_fact_sinistres ON c.contrat_id = s.contrat_id;
# MAGIC
# MAGIC -- 4. Matrice des Doublons (Tableau filtré)
# MAGIC
# MAGIC SELECT master_client_id, Nom_Hached, Prenom_Hached, COUNT(*) as nombre_de_comptes
# MAGIC FROM gold_dim_client
# MAGIC GROUP BY master_client_id, Nom_Hached, Prenom_Hached
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# DBTITLE 1,PROPOSITIONS DE SONAR_TESTS
# ============================================================
# STEP 1 : CONFIGURATION ET DÉMARRAGE IMMÉDIAT DU COVERAGE
# ============================================================
import sys
import os
from coverage import Coverage

# Définir le chemin absolu du dossier SRC
src_path = "/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/TEST_SONAR_GIT/SRC"

test_path = "/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/TEST_SONAR_GIT/TESTS"

if src_path not in sys.path:
    sys.path.append(src_path)

if test_path not in sys.path:
   sys.path.append(test_path)



# On initialise et on démarre TOUT DE SUITE le coverage, avant tout import !
cov = Coverage(source=[src_path])
cov.start()

# ==========================================
# STEP 2 : IMPORTS PROPRES DES MODULES (PAS de exec)
# ==========================================
# Au lieu d'utiliser exec(), utilisez importlib pour charger les fichiers comme de vrais modules Python.
import pkgutil
import importlib

print("Importation des modules (SRC) sous surveillance de coverage...")
for module_info in pkgutil.iter_modules([src_path]):
    # Charge le fichier proprement en tant que module Python valide
    # reload : recharge le module s'il est déjà en mémoire, pour prendre en compte les modifications du fichier
    module = importlib.import_module(module_info.name)
    module = importlib.reload(module)
    
    # Injecte les fonctions dans l'espace global pour que vos tests unittest les trouvent
    module_attrs = [attr for attr in dir(module) if not attr.startswith('_')]
    for attr in module_attrs:
        globals()[attr] = getattr(module, attr)
# Pour les tests : les fichiers de TESTS sont chargés via exec() (la couverture ne porte que sur SRC).
# exec() fait partager le namespace du notebook, ce qui est indispensable ici car les fonctions
# de test s'appellent entre elles et utilisent les DataFrames créés par les fichiers d'ingestion.
import glob

print("Chargement des fichiers de tests (TESTS)...")
test_files = sorted(glob.glob(os.path.join(test_path, "SONAR_TEST_*.py")))
# Les fichiers d'ingestion en premier : ils créent df_ab_* utilisés par les autres tests
ingestion_files = [f for f in test_files if "INGESTION" in os.path.basename(f)]
other_files = [f for f in test_files if f not in ingestion_files]

for f in ingestion_files + other_files:
    try:
        exec(open(f).read())
    except Exception as e:
        print(f"ATTENTION : erreur au chargement de {os.path.basename(f)} : {e}")

# Plusieurs tests utilisent df_ab_* : on les crée via la fonction SRC
# (dans le fichier TESTS d'ingestion Oracle, la création au niveau module est dans un docstring = code mort)
df_ab_clients, df_ab_contrats, df_ab_sinistres, df_ab_payments = create_ab_dataframes(spark)
# Idem pour les DataFrames AssurePlus (df_ap_*), utilisés notamment par harmonize_clients
df_ap_clients, df_ap_contrats, df_ap_sinistres, df_ap_payments = load_ap_data(spark)

# Import des fonctions de tests
import unittest
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Charger vos fichiers de tests (Ici vous pouvez garder exec() UNIQUEMENT pour les scripts de tests,
# car nous ne cherchons pas à mesurer la couverture des fichiers de tests eux-mêmes, mais celle de SRC)
# je les commente
#exec(open("/Workspace/Users/kacidatabricks@://gmail.com").read())
#exec(open("/Workspace/Users/kacidatabricks@://gmail.com").read())

# ... (Gardez vos autres lignes d'exécution de fichiers de TESTS ici) ...
'''
#FONC
exec(open("/Workspace/Users/kacidatabricks@gmail.com/PROJET_BIG_DATA_STUDI/SRC/FONC_INGESTION_DONNEES_ORACLE.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/PROJET_BIG_DATA_STUDI/SRC/FONC_INGESTION_DES_DONNES_ASSUREPLUS.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/PROJET_BIG_DATA_STUDI/SRC/FONC_GARDERLATRACE.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/PROJET_BIG_DATA_STUDI/SRC/FONC_ENREGISTREMENT_EN_BRONZE.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/PROJET_BIG_DATA_STUDI/SRC/FONC_ENREGISTREMENT_EN_SILVER.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/PROJET_BIG_DATA_STUDI/SRC/FONC_NETTOYAGE_DES_DEUX_BASES.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/PROJET_BIG_DATA_STUDI/SRC/FONC_HARMONISATION_ET_UNIFICATION_CLIENTS.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/PROJET_BIG_DATA_STUDI/SRC/FONC_MATRICE_DOUBLONS.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/PROJET_BIG_DATA_STUDI/SRC/FONC_GOLD_CLIENT.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/PROJET_BIG_DATA_STUDI/SRC/FONC_UNIFICATION_ET_GOLD_CONTRAT.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/PROJET_BIG_DATA_STUDI/SRC/FONC_UNIFICATION_ET_GOLD_SINISTRE.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/PROJET_BIG_DATA_STUDI/SRC/FONC_UNIFICATION_ET_GOLD_PAYMENT.py").read())

'''
'''
# TEST
exec(open("/Workspace/Users/kacidatabricks@gmail.com/PROJET_BIG_DATA_STUDI/TESTS/TEST_INGESTION_DONNEES_ORACLE.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/PROJET_BIG_DATA_STUDI/TESTS/TEST_INGESTION_DES_DONNES_ASSUREPLUS.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/PROJET_BIG_DATA_STUDI/TESTS/TEST_GARDER_LA_TRACE.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/PROJET_BIG_DATA_STUDI/TESTS/TEST_ENREGISTREMENT_EN_BRONZE.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/PROJET_BIG_DATA_STUDI/TESTS/TEST_ENREGISTREMENT_EN_SILVER.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/PROJET_BIG_DATA_STUDI/TESTS/TEST_NETTOYAGE_DES_DEUX_BASES.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/PROJET_BIG_DATA_STUDI/TESTS/TEST_HARMONISATION_ET_UNIFICATION_CLIENTS.py").read())
exec(open("/Workspace/Users/kacidatabricks@gmail.com/PROJET_BIG_DATA_STUDI/TESTS/TEST_MATRICE_DOUBLONS.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/PROJET_BIG_DATA_STUDI/TESTS/TEST_GOLD_CLIENT.py").read())

# exec(open("/Workspace/Users/kacidatabricks@gmail.com/PROJET_BIG_DATA_STUDI/TESTS/TEST_UNIFICATION_ET_GOLD_CONTRAT.py").read())  # File not found - skipped

exec(open("/Workspace/Users/kacidatabricks@gmail.com/PROJET_BIG_DATA_STUDI/TESTS/TEST_UNIFICATION_ET_GOLD_SINISTRE.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/PROJET_BIG_DATA_STUDI/TESTS/TEST_UNIFICATION_ET_GOLD_PAYMENT.py").read())

'''


# ==========================================
# STEP 3 : EXÉCUTION DE la  CLASSE DE TEST
# ==========================================

# Fonction clean_email utilisée par test_clean_email
def clean_email(value):
    if value is None:
        return None
    return value.strip().lower()

# Alias : le test appelle generer_matrice_doublons (en minuscules),
# or la fonction SRC s'appelle generer_Matrice_Doublons
#generer_matrice_doublons = generer_matrice_oublons

class TestETLPipeline(unittest.TestCase):
    def test_clean_email(self):
        self.assertEqual(clean_email("  JEAN.DUPONT@Email.FR "), "jean.dupont@email.fr")
        self.assertIsNone(clean_email(None))
        
    def test_clean_last_name(self):
        df = df_ab_clients.withColumn("AB_NOM", F.lit("   martin "))
        df_clean = clean_last_name(df, "AB_NOM")
        value = df_clean.select("AB_NOM").collect()[0][0]
        self.assertEqual(value, "MARTIN")
    
    # ... (Conservez le reste de vos méthodes de tests inchangé) ...

    '''
    def test_unified_clients(self):
        self.assertIsInstance(df_clients, DataFrame)
        self.assertTrue(df_clients.count() > 0)
    '''
    '''
    def test_clean_last_name(self):
        df = df_ab_clients.withColumn("AB_NOM", F.lit("   martin "))
        df_clean = clean_last_name(df, "AB_NOM")
        value = df_clean.select("AB_NOM").collect()[0][0]
        self.assertEqual(value, "MARTIN")
    '''

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

# ajoutés 16102026

    def test1_create_ab_dataframes(self):
        test_create_ab_dataframes()

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
        # Le test de nettoyage attend le schéma silver (NOM, PRENOM, EMAIL, ADRESSE, CODE_POSTAL, VILLE...)
        # On reproduit l'ordre du pipeline : métadonnées -> unification -> nettoyage
        # (harmonize_clients a besoin des colonnes INGESTION_TIMESTAMP et SOURCE_FILE ajoutées par ajoutmétadonnées)
        df_ab_meta = ajoutmétadonnées(df_ab_clients, "Oracle", "oracle_clients.csv", "AB_CLIENT_ID")
        df_ap_meta = ajoutmétadonnées(df_ap_clients, "SQL Server", "assureplus_clients.csv", "AP_USER_ID")
        df_unified = harmonize_clients(df_ab_meta, df_ap_meta).withColumn("VILLE", F.lit("Paris"))
        #df = test_nettoyage_des_deux_bases(df_unified)
        df=data_cleaning(df_ab_meta,df_ap_meta)

'''  ancienne 
suite = unittest.TestLoader().loadTestsFromTestCase(TestETLPipeline)
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)

# ancienne
'''

# TestLoader().loadTestsFromModule va automatiquement chercher les classes Test de votre environnement
loader = unittest.TestLoader()
suite = loader.loadTestsFromModule(sys.modules[__name__])

# 6. Exécution
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)

# 7. Arrêt et Rapport
cov.stop()
cov.save()

print("\n--- RAPPORT DE COUVERTURE OPTIMISÉ ---")
cov.report(ignore_errors=True)


'''  ancienne 
# ==========================================
# STEP 4 : ARRÊT ET RAPPORT
# ==========================================
cov.stop()
cov.save()

print("\n--- RAPPORT DE COUVERTURE DES TESTS ---")
cov.report(ignore_errors=True)

# ancienne
'''


# COMMAND ----------

# DBTITLE 1,SONAR_TESTS
# 1. Activation de l'autoreload pour prendre en compte les modifs de vos fichiers .py
%load_ext autoreload
%autoreload 2

import sys
import os
import pkgutil
import importlib

# 2. Configuration du chemin vers votre dossier 'src'
current_dir = os.getcwd()
src_path = os.path.abspath(os.path.join(current_dir, "src"))

src_path = "/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/src"

print("src_path:  ",src_path)
if src_path not in sys.path:
    sys.path.append(src_path)

SONAR_TEST_path = "/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/SONAR_TEST"

if SONAR_TEST_path not in sys.path:
    sys.path.append(SONAR_TEST_path)

# 3. Import automatique et massif de TOUTES les SONAR_FONCtions de TOUS les fichiers
for module_info in pkgutil.iter_modules([src_path]):
    # Importer le fichier .py en tant que module
    module = importlib.import_module(module_info.name)
    
    # Récupérer tout le contenu public du fichier
    module_attrs = [attr for attr in dir(module) if not attr.startswith('_')]
    
    # Injecter les SONAR_FONCtions directement dans l'espace global du notebook
    for attr in module_attrs:
        globals()[attr] = getattr(module, attr)

print("✅ Toutes les SONAR_FONCtions ont été importées avec succès ! Vous pouvez les utiliser directement.")



import unitSONAR_TEST
from pyspark.sql import DataFrame
#from pyspark.sql import functions as F


#SONAR_FONC
exec(open("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/SRC/SONAR_FONC_INGESTION_DONNEES_ORACLE.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/SRC/SONAR_FONC_INGESTION_DES_DONNES_ASSUREPLUS.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/SRC/SONAR_FONC_GARDERLATRACE.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/SRC/SONAR_FONC_ENREGISTREMENT_EN_BRONZE.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/SRC/SONAR_FONC_ENREGISTREMENT_EN_SILVER.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/SRC/SONAR_FONC_NETTOYAGE_DES_DEUX_BASES.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/SRC/SONAR_FONC_HARMONISATION_ET_UNIFICATION_CLIENTS.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/SRC/SONAR_FONC_MATRICE_DOUBLONS.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/SRC/SONAR_FONC_GOLD_CLIENT.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/SRC/SONAR_FONC_UNIFICATION_ET_GOLD_CONTRAT.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/SRC/SONAR_FONC_UNIFICATION_ET_GOLD_SINISTRE.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/SRC/SONAR_FONC_UNIFICATION_ET_GOLD_PAYMENT.py").read())

# SONAR_TEST
exec(open("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/SONAR_TESTS/SONAR_TEST_INGESTION_DONNEES_ORACLE.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/SONAR_TESTS/SONAR_TEST_INGESTION_DES_DONNES_ASSUREPLUS.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/SONAR_TESTS/SONAR_TEST_GARDER_LA_TRACE.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/SONAR_TESTS/SONAR_TEST_ENREGISTREMENT_EN_BRONZE.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/SONAR_TESTS/SONAR_TEST_ENREGISTREMENT_EN_SILVER.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/SONAR_TESTS/SONAR_TEST_NETTOYAGE_DES_DEUX_BASES.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/SONAR_TESTS/SONAR_TEST_HARMONISATION_ET_UNIFICATION_CLIENTS.py").read())
exec(open("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/SONAR_TESTS/SONAR_TEST_MATRICE_DOUBLONS.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/SONAR_TESTS/SONAR_TEST_GOLD_CLIENT.py").read())

# exec(open("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/SONAR_TESTS/SONAR_TEST_UNIFICATION_ET_GOLD_CONTRAT.py").read())  # File not found - skipped

exec(open("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/SONAR_TESTS/SONAR_TEST_UNIFICATION_ET_GOLD_SINISTRE.py").read())

exec(open("/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/SONAR_TESTS/SONAR_TEST_UNIFICATION_ET_GOLD_PAYMENT.py").read())









# Pour ouvrir tous les fichiers en py de src
#import os

#src_path = "/Workspace/Users/kacidatabricks@gmail.com/ des src et notebooks abassurance_databricks_bloc3_kit/SRC/"
# files = [f for f in os.listdir(src_path) if f.endswith(".py")]

# for fname in files:
#    exec(open(os.path.join(src_path, fname)).read())


#      IIIIIIIIIII je veux SONAR_TESTer le coverage sur le premier paquet de SONAR_TEST IIIIIIIIII

import sys
from coverage import Coverage

# 1. Initialiser et démarrer la capture de couverture
cov = Coverage(source=["/Workspace/Users/kacidatabricks@gmail.com/TEST_SONAR_GIT/SRC"])
# cov = Coverage()
cov.start()



def clean_email(value):
    if value is None:
        return None
    return value.strip().lower()

class SONAR_TESTETLPipeline(unitSONAR_TEST.SONAR_TESTCase):
    def SONAR_TEST_clean_email(self):
        self.assertEqual(clean_email("  JEAN.DUPONT@Email.FR "), "jean.dupont@email.fr")
        self.assertIsNone(clean_email(None))

    def SONAR_TEST_ajoutetadonnees(self):
        df = df_ab_clients.limit(1)
        df_meta = ajoutmétadonnées(df, "Oracle", "file.csv", "AB_CLIENT_ID")
        columns = df_meta.columns
        for colname in ["SOURCE_SYSTEM", "ORIGINAL_ID", "SOURCE_FILE", "INGESTION_TIMESTAMP"]:
            self.assertIn(colname, columns)
    '''
    def SONAR_TEST_unified_clients(self):
        self.assertIsInstance(df_clients, DataFrame)
        self.assertTrue(df_clients.count() > 0)
    '''
    def SONAR_TEST_clean_last_name(self):
        df = df_ab_clients.withColumn("AB_NOM", F.lit("   martin "))
        df_clean = clean_last_name(df, "AB_NOM")
        value = df_clean.select("AB_NOM").collect()[0][0]
        self.assertEqual(value, "MARTIN")

    def SONAR_TEST_normalize_firstname(self):
        df = df_ab_clients.withColumn("AB_PRENOM", F.lit("  jean "))
        df_norm = df.withColumn("AB_PRENOM", normalize_firstname("AB_PRENOM"))
        value = df_norm.select("AB_PRENOM").collect()[0][0]
        self.assertEqual(value, "JEAN")

    def SONAR_TEST_normalize_phone_number(self):
        df = df_ab_clients.withColumn("AB_TELEPHONE", F.lit("0612345678"))
        df_norm = normalize_phone_number(df, "AB_TELEPHONE")
        value = df_norm.select("AB_TELEPHONE").collect()[0][0]
        self.assertTrue(value.startswith("+33"))

    def SONAR_TEST_matching_key(self):
        self.assertIn("MATCH_KEY", matched_clients.columns)
        keys = matched_clients.select("MATCH_KEY").distinct().count()
        self.assertTrue(keys > 0)

    def SONAR_TEST_master_client_id_uniqueness(self):
        count_id = dim_clients.select("MASTER_CLIENT_ID").distinct().count()
        self.assertTrue(count_id > 0)

    def SONAR_TEST_gold_fact_contrat(self):
        self.assertIsInstance(fact_contrats_gold, DataFrame)
        self.assertTrue(fact_contrats_gold.count() > 0)

    def SONAR_TEST_gold_fact_sinistres(self):
        self.assertIsInstance(fact_sinistres_gold, DataFrame)
        self.assertTrue(fact_sinistres_gold.count() > 0)

    def SONAR_TEST_doublons(self):
        df = doublons_df
        self.assertTrue("MASTER_CLIENT_ID" in df.columns)
        self.assertTrue(df.count() >= 0)

# ajoutés 16102026

    def SONAR_TEST1_create_ab_dataframes(self):
        SONAR_TEST_create_ab_dataframes()

    #SONAR_TEST_load_ap_data()
    def SONAR_TEST1_enregistrement_bronze(self):
      SONAR_TEST_enregistrement_bronze()
    def SONAR_TEST1_ajoutmétadonnées(self) :
       SONAR_TEST_ajoutmétadonnées()
# mettre ici le SONAR_TEST de nettoyage
    def SONAR_TEST1_enregistrement_silver(self):
       SONAR_TEST_enregistrement_silver()
# SONAR_TEST_harmonize_clients()
    def SONAR_TEST1_generer_matrice_doublons(self):
      SONAR_TEST_generer_matrice_doublons()
    def SONAR_TEST1_save_dim_client_to_gold(self):
      SONAR_TEST_save_dim_client_to_gold()
# SONAR_TEST_SONAR_unify_and_map_contrats()  # SONAR_TEST file not found - skipped
    def SONAR_TEST1_SONAR_unify_sinistres(self):
      SONAR_TEST_SONAR_unify_sinistres()
    def SONAR_TEST1_SONAR_unify_payments(self):
      SONAR_TEST_SONAR_unify_payments()


suite = unitSONAR_TEST.SONAR_TESTLoader().loadSONAR_TESTsFromSONAR_TESTCase(SONAR_TESTETLPipeline)
runner = unitSONAR_TEST.TextSONAR_TESTRunner(verbosity=2)
runner.run(suite)
# The rest of the text is commented out as per instructions


# IIIIIIIIIIIIIIIIIIIIIIIIIIje l ai rajouté pour SONAR_TESTer le premier pasuet avec coverage
cov.stop()
cov.save()

# 4. Afficher le rapport directement dans l'output de la cellule Databricks
print("\n--- RAPPORT DE COUVERTURE DES SONAR_TESTS ---")
cov.report(ignore_errors=True)

#    FIN  IIIIIIIIIIIIIIIIIIIIIIIIIIje l ai rajouté pour SONAR_TESTer le premier pasuet avec coverage



#  IIIIIIII  Le COVERAGE  ###########################
'''
import sys
from coverage import Coverage

# 1. Initialiser et démarrer la capture de couverture
cov = Coverage()
cov.start()

# -------------------------------------------------------------
# Vos SONAR_FONCtions de SONAR_TESTs (Exemple)
# -------------------------------------------------------------
def SONAR_TEST_ma_SONAR_FONCtion_1():
    # Remplacez par votre vrai appel et assert
    assert ma_SONAR_FONCtion_a_SONAR_TESTer(2) == 4

def SONAR_TEST_ma_SONAR_FONCtion_2():
    assert ma_SONAR_FONCtion_a_SONAR_TESTer(0) == 0

# -------------------------------------------------------------
# Exécution manuelle des SONAR_FONCtions de SONAR_TEST dans la cellule
# -------------------------------------------------------------
try:
    print("Démarrage des SONAR_TESTs...")
    t = SONAR_TESTETLPipeline()
    t.SONAR_TEST_clean_email()
    t.SONAR_TEST_ajoutetadonnees()
    #t.SONAR_TEST_unified_clients()
    t.SONAR_TEST_clean_last_name()
    t.SONAR_TEST_normalize_firstname()
    t.SONAR_TEST_normalize_phone_number()
    t.SONAR_TEST_matching_key()
    t.SONAR_TEST_master_client_id_uniqueness()
    t.SONAR_TEST_gold_fact_contrat()
    t.SONAR_TEST_gold_fact_sinistre()
    t.SONAR_TEST_doublons()
# Ajout de nouveaux SONAR_TESTs##############
  
    SONAR_TEST_create_ab_dataframes()
    SONAR_TEST_load_ap_data()
    SONAR_TEST_enregistrement_bronze()
    SONAR_TEST_ajoutmétadonnées()
    # mettre ici le SONAR_TEST de nettoyage
    SONAR_TEST_enregistrement_silver()
    # SONAR_TEST_harmonize_clients()
    SONAR_TEST_generer_MATRICE_DOUBLONS()
    SONAR_TEST_save_dim_client_to_gold()
    # SONAR_TEST_SONAR_unify_and_map_contrats()  # SONAR_TEST file not found - skipped

    SONAR_TEST_SONAR_unify_sinistres()
    SONAR_TEST_SONAR_unify_payments()

    print("Tous les SONAR_TESTs ont réussi !")
except AssertionError as e:
    print(f"Échec d'un SONAR_TEST : {e}")
finally:
    # 3. Arrêter la capture même si un SONAR_TEST échoue
    cov.stop()
    cov.save()

# 4. Afficher le rapport directement dans l'output de la cellule Databricks
print("\n--- RAPPORT DE COUVERTURE DES SONAR_TESTS ---")
cov.report(ignore_errors=True)

'''


# COMMAND ----------

# DBTITLE 1,DASHBOARD
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
# MAGIC
# MAGIC /*
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
# MAGIC LEFT JOIN workspace.default.gold_fact_sinistres 
# MAGIC     ON c.CONTRAT_ID = s.CONTRAT_ID;
# MAGIC
# MAGIC -- 4. Matrice des doublons (Table Filter)
# MAGIC SELECT MASTER_CLIENT_ID, Nom_Hached, Prenom_Hached, COUNT(*) AS nb_comptes
# MAGIC FROM workspace.default.gold_dim_client
# MAGIC GROUP BY MASTER_CLIENT_ID, Nom_Hached, Prenom_Hached
# MAGIC HAVING COUNT(*) > 1;
# MAGIC
# MAGIC --SQL
# MAGIC --0. Création de l'information total_clients
# MAGIC SELECT COUNT(DISTINCT MASTER_CLIENT_ID) as total_clients
# MAGIC FROM gold_dim_client;
# MAGIC */
# MAGIC -- 1. Répartition des clients par Ville (Graphique Camembert / Pie Chart)
# MAGIC SELECT CODE_POSTAL, COUNT(DISTINCT MASTER_CLIENT_ID) as total_clients
# MAGIC FROM gold_dim_clients
# MAGIC GROUP BY CODE_POSTAL;
# MAGIC
# MAGIC /*
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
# MAGIC LEFT JOIN gold_fact_sinistres ON c.CONTRAT_ID = s.CONTRAT_ID;
# MAGIC
# MAGIC
# MAGIC -- 4. Matrice des Doublons (Tableau filtré)
# MAGIC SELECT MASTER_CLIENT_ID, Nom_Haxhed, Prenom_Haxhed, COUNT(*) as nombre_de_comptes
# MAGIC FROM gold_dim_client
# MAGIC GROUP BY MASTER_CLIENT_ID, Nom_Hached, Prenom_Hached
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
# MAGIC FROM gold_fact_sinistres
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
# MAGIC */
# MAGIC -- 3. Statistiques sinistres & financières
# MAGIC SELECT 
# MAGIC     SUM(c.MONTANT_PRIME) AS chiffre_affaires,
# MAGIC     SUM(s.MONTANT_INDEMNISE) AS indemnites_versees,
# MAGIC     ROUND((SUM(s.MONTANT_INDEMNISE) / SUM(c.MONTANT_PRIME)) * 100, 2) AS ratio_sinistralite_pct
# MAGIC FROM gold_fact_contrat c
# MAGIC LEFT JOIN gold_fact_sinistres 
# MAGIC     ON c.CONTRAT_ID = s.CONTRAT_ID;
# MAGIC /*
# MAGIC -- 4. Matrice des doublons
# MAGIC SELECT MASTER_CLIENT_ID, Nom_Hached, Prenom_Hached, COUNT(*) AS nb_comptes
# MAGIC FROM gold_dim_client
# MAGIC GROUP BY MASTER_CLIENT_ID, Nom_Hached, Prenom_Hached
# MAGIC HAVING COUNT(*) > 1;
# MAGIC
# MAGIC -- 5. Contrats par statut
# MAGIC SELECT STATUT_CONTRAT, COUNT(*) AS nb_contrats
# MAGIC FROM gold_fact_contrat
# MAGIC GROUP BY STATUT_CONTRAT;
# MAGIC
# MAGIC -- 6. Sinistres par statut
# MAGIC SELECT STATUT_SINISTRE, COUNT(*) AS nb_sinistres, SUM(MONTANT_INDEMNISE) AS montant_total
# MAGIC FROM gold_fact_sinistres
# MAGIC GROUP BY STATUT_SINISTRE;
# MAGIC
# MAGIC -- 7. Total primes
# MAGIC SELECT SUM(MONTANT_PRIME) AS primes
# MAGIC FROM gold_fact_contrat;
# MAGIC
# MAGIC -- 8. Total paiements
# MAGIC SELECT SUM(MONTANT) AS paiements
# MAGIC FROM gold_fact_payment;
# MAGIC */
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
# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# # test couverture
Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
%restart_python
 

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
# test du 13 25
#Test 1318 31110
# # test couverture du 31 10 2026
# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
%restart_python
# oooo
# dernier essai
# fonctionne enfin?
#Test avec le nouveau yml pour les raison de sécutité
#Troisième commit
# ttttt
# Deuxième commit
# # Premier commit
"""
PARTIE 3 : IMPLEMENTATION TECHNIQUE SUR DATABRICKS (Sujet 3)
Dossier 1 : Structure Git et Modèle de Données
Question 1 : Repository Git
	Branches : main (production), develop (intégration), feature/ingestion-oracle, feature/deduplication, bugfix/date-format.

	Convention de nommage : <type>/<ticket-id>-<description-courte> (Ex: feature/AB-102-pipeline-sinistres).

Question 2 : Modèle Conceptuel des Données (MCD Unifié)
Le modèle réconcilie la structure d'AbAssurance (Oracle) et d'AssurePlus (SQL Server) : 


+-------------------------------------------------+
|                  DIM_CLIENTS                     |
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
|                  FACT_CONTRATS                   |
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
|                  FACT_SINISTRES                  |
+-------------------------------------------------+
| sinistre_id (PK)                                |
| contrat_id (FK)                                 |
| date_sinistre, date_declaration                 |
| type_sinistre, statut ('OUVERT', 'CLOS'...)    |
| montant_indemnise, cout_estime                  |
+-------------------------------------------------+
Dossier 2 & 3 : Implémentation complète sous Databricks (PySpark)
"""
# 2. Analysez le code d'une cellule ou d'un fichier script spécifique
#%pip install pylint
'''
import pylint.lint
# 1. On s'assure que le linter est installé
%pip install pylint --quiet

# 2. On utilise l'outil magique "!" pour forcer l'analyse système du fichier
!pylint "O(Clone)_GEM_AVEC_Nettoyage_et_AVC_schem_et_vrAP_Copie.py" --max-line-length=150
pylint_opts = ["--max-line-length=150", "O(Clone)_GEM_AVEC_Nettoyage_et_AVC_schem_et_vrAP_Copie.py"]
pylint.lint.Run(pylint_opts)
'''

# POUR LE COVERAGE ########################
%pip install coverage

#   CONTRATs ET SIISTRES ET PAIEMENTS   et CLIENTS AUSSI    créer des tables silver 

# Étape 1 : Simulation des jeux de données Oracle & SQL Server et enregistrementen bronze


from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap, col, lower, upper, trim, concat_ws, levenshtein, when, lit, count, sum as _sum, avg, round, to_date
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
from datetime import datetime
from pyspark.sql.window import Window


from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, DoubleType
from pyspark.sql import functions as F


# Initialisation Spark
# spark = SparkSession.builder.appName("AssuranceData").getOrCreate()   Apparemment non nécessaire


# Importer des SONAR_FONCtions de SRC
import sys
import os

# 1. Récupérer le chemin absolu du dossier courant (NOTEBOOK)
current_dir = os.getcwd()

# 2. CORRECTION : Remonter d'un niveau (dossier parent) avant d'aller dans SRC
src_path = os.path.abspath(os.path.join(current_dir, "..", "SRC"))

print("----SRC path -------------", src_path)

# 3. L'ajouter au chemin système
if src_path not in sys.path:
    sys.path.append(src_path)

print("SRRRRRRRRRRRRRRRRC")
# Données oracle

# 4. Importer la SONAR_FONCtion

from SONAR_FONC_INGESTION_DONNEES_ORACLE import create_ab_dataframes


df_ab_clients, df_ab_contrats, df_ab_sinistres, df_ab_payments = create_ab_dataframes(spark)
# Affichage des Dataframes
print(df_ab_clients)
print(df_ab_contrats)
print(df_ab_sinistres)
print(df_ab_payments)


"""
# Création des DataFrames

df_ab_clients = spark.createDataFrame(ab_clients_data, schema=ab_clients_schema)
"""

# Protection des données
def protection_bronze(df2, source):
    

    salt = dbutils.secrets.get(scope="PROJET_BIG_DATA_STUDI", key="hash_salt")
    
    if source == "Oracle":
    

     df2 = df2.withColumns({"Email_Hached": F.sha2(F.concat(F.col("AB_EMAIL"), F.lit(salt)), 256),

     "Nom_Hached": F.sha2(F.concat(F.col("AB_NOM"), F.lit(salt)), 256),
     "Prenom_Hached": F.sha2(F.concat(F.col("AB_PRENOM"), F.lit(salt)), 256),
     "Date_de_naissance_Hached": F.sha2(F.concat(F.col("AB_DATE_NAISSANCE"), F.lit(salt)), 256)}).drop("AB_NOM", "AB_PRENOM","AB_EMAIL","AB_DATE_NAISSANCE")
    else: 
    
     df2 = df2.withColumns({"Email_Hached": F.sha2(F.concat(F.col("AP_MAIL_ADDRESS"), F.lit(salt)), 256),
     "FULL_NAME_Hached": F.sha2(F.concat(F.col("AP_FULL_NAME"), F.lit(salt)), 256),
     "Nom_Hached": F.sha2(F.concat(F.col("AP_NOM"), F.lit(salt)), 256),
     "Prenom_Hached": F.sha2(F.concat(F.col("AP_PRENOM"), F.lit(salt)), 256),
     "Date_de_naissance_Hached": F.sha2(F.concat(F.col("AP_BIRTH_DATE"), F.lit(salt)), 256)}).drop("AP_FULL_NAME","AP_MAIL_ADDRESS","AP_BIRTH_DATE","AP_NOM", "AP_PRENOM")
    return df2

# Protection bronze

df_ab_clients_bronze = protection_bronze(df_ab_clients,"Oracle")

print("/n est ce que df_ab_clients est protégé à la sortie de la SONAR_FONCtion protection_bronze ", df_ab_clients)



# Ajout de sécurité du 30 0 9 2026


# Affichage des DataFrames pour voir s'il n'a pas été modifi" par la SONAR_FONCtion protection_bronze
print("=== df_ab_clients ===")
df_ab_clients.show(10, truncate=False)

# ajout display de ab clinet protege de e mail le 30 0 9 2026
print("=== df_ab_clients_protege ===")
df_ab_clients_bronze.show(10, truncate=False)


print("=== df_ab_contrats ===")
df_ab_contrats.show(10, truncate=False)

print("=== df_ab_sinistres ===")
df_ab_sinistres.show(10, truncate=False)

print("=== df_ab_payments ===")
df_ab_payments.show(10, truncate=False)


# Ingestion des données SQLServer

# Données AssurPlus SQLserver

# Import load_ap_data 
from SONAR_FONC_INGESTION_DES_DONNES_ASSUREPLUS import load_ap_data

# Obtention des dataframes
df_ap_clients, df_ap_contrats, df_ap_sinistres, df_ap_payments = load_ap_data()

 # Scinder en nom et prénom le full name de Assurplus
def split_full_name(df,col_last_name, col_first_name,col_full_name):

 df = df.withColumn(col_last_name, F.trim(F.split(F.col(col_full_name), " ").getItem(1))) \
             .withColumn(col_first_name, F.trim(F.split(F.col(col_full_name), " ").getItem(0)))
 return df
 # Extraire le nom et prénom à partir du Full_name
df_ap_clients = split_full_name(df_ap_clients,"AP_NOM","AP_PRENOM","AP_FULL_NAME")


# Protection des données

df_ap_clients_bronze = protection_bronze(df_ap_clients,"SQLServer")



# Affichage df_pa_clients_bronze 
print("=== ASSUREPLUS - Clients ===")
df_ap_clients_bronze.show(10, truncate=False)

print("=== ASSUREPLUS - Contrats ===")
df_ap_contrats.show(10, truncate=False)

print("=== ASSUREPLUS - Sinistres ===")
df_ap_sinistres.show(10, truncate=False)

print("=== ASSUREPLUS - Payments ===")
df_ap_payments.show(10, truncate=False)


# Enregistrement bronze
from SONAR_FONC_ENREGISTREMENT_EN_BRONZE import enregistrement_bronze

df_ab_contrats_bronze = df_ab_contrats
df_ab_sinistres_bronze = df_ab_sinistres
df_ab_payments_bronze= df_ab_payments

df_ap_contrats_bronze = df_ap_contrats
df_ap_sinistres_bronze = df_ap_sinistres
df_ap_payments_bronze= df_ap_payments

#                                   ENREGISTREMENT EN BRONZE        
# ATTENTION: Toutes les tables ne sont pas enregistrées dans la base de données delta
'''
def enregistrement_bronze(df,tablename):

 df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tablename)
'''
# Tables Oracles
enregistrement_bronze(df_ab_clients_bronze,"workspace.bronze.oracle_clients")
enregistrement_bronze(df_ab_contrats_bronze,"workspace.bronze.oracle_contrats")
enregistrement_bronze(df_ab_sinistres_bronze,"workspace.bronze.oracle_sinistres")
enregistrement_bronze(df_ab_payments_bronze,"workspace.bronze.oracle_payments")

# Tables SQL Server
enregistrement_bronze(df_ap_clients_bronze,"workspace.bronze.sqlserver_clients")
enregistrement_bronze(df_ap_contrats_bronze,"workspace.bronze.sqlserver_contrats")
enregistrement_bronze(df_ap_payments_bronze,"workspace.bronze.sqlserver_payments")
enregistrement_bronze(df_ap_sinistres_bronze,"workspace.bronze.sqlserver_sinistres")



#   ICI Commence le silver ***************************************

from SONAR_FONC_ENREGISTREMENT_EN_SILVER import enregistrement_silver

# Protection des données
def protection_silver(df2, source):
    

    salt = dbutils.secrets.get(scope="PROJET_BIG_DATA_STUDI", key="hash_salt")
    
    if source == "Oracle":
    

     df2 = df2.withColumns({"Email_Hached": F.sha2(F.concat(F.col("AB_EMAIL"), F.lit(salt)), 256),

     "Nom_Hached": F.sha2(F.concat(F.col("AB_NOM"), F.lit(salt)), 256),
     "Prenom_Hached": F.sha2(F.concat(F.col("AB_PRENOM"), F.lit(salt)), 256),
     "Date_de_naissance": F.sha2(F.concat(F.col("AB_DATE_NAISSANCE"), F.lit(salt)), 256)}).drop("AB_NOM", "AB_PRENOM","AB_EMAIL","AB_DATE_NAISSANCE")
     return df2
    else: 
    
     df2 = df2.withColumns({"Email_Hached": F.sha2(F.concat(F.col("AP_MAIL_ADDRESS"), F.lit(salt)), 256),
     "FULL_NAME_Hached": F.sha2(F.concat(F.col("AP_FULL_NAME"), F.lit(salt)), 256),
     "Nom_Hached": F.sha2(F.concat(F.col("AP_NOM"), F.lit(salt)), 256),
     "Prenom_Hached": F.sha2(F.concat(F.col("AP_PRENOM"), F.lit(salt)), 256),
     "Date_de_naissance_Hached": F.sha2(F.concat(F.col("AP_BIRTH_DATE"), F.lit(salt)), 256)}).drop("AP_FULL_NAME","AP_MAIL_ADDRESS","AP_BIRTH_DATE","AP_NOM", "AP_PRENOM")
     return df2


# Ajout des données pour avoir la trace

#from pyspark.sql import functions as F

# 1. Définition des SONAR_FONCtions
'''
def ajoutmétadonnées(df, source_system, source_file, client_id_col):
    return df \
        .withColumn("SOURCE_SYSTEM", F.lit(source_system)) \
        .withColumn("ORIGINAL_ID", F.col(client_id_col).cast("string")) \
        .withColumn("SOURCE_FILE", F.lit(source_file)) \
        .withColumn("INGESTION_TIMESTAMP", F.current_timestamp())

'''

# 2. Exécution de la chaîne (Exemple avec un DataFrame initialisé)
# oracle_clients = spark.read...

from SONAR_FONC_GARDER_LA_TRACE import ajoutmétadonnées


# ORACLE
df_ab_clients =ajoutmétadonnées (df_ab_clients,"Oracle","Oracle_clients.csv","AB_CLIENT_ID")

#SQL Server
df_ap_clients =ajoutmétadonnées (df_ap_clients,"SQL","SQL_clients.csv","AP_USER_ID")


display(df_ab_clients)
display(df_ap_clients)






# Étape 2 : Data Pipeline - Harmonisation & Unification (Silver Layer)

"""

Schéma client final :

MASTER_CLIENT_ID
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



from SONAR_FONC_NETTOYAGE_DES_DEUX_BASES import data_cleaning



df_ab_clients, df_ap_clients= data_cleaning(df_ab_clients,df_ap_clients)

######   ENREGISTREMENT SILVER #############################
#print("Colonnes AB :", df_ab_clients.columns)
#print("Colonnes AP :", df_ap_clients.columns)
  
from SONAR_FONC_ENREGISTREMENT_EN_SILVER import enregistrement_silver


#                                   ENREGISTREMENT EN SILVER        
# ATTENTION: Toutes les tables ne sont pas enregistrées dans la base de données 



df_ab_clients_silver = protection_silver(df_ab_clients,"Oracle")
 # à faire drop 
df_ab_contrats_silver = df_ab_contrats
df_ab_sinistres_silver = df_ab_sinistres
df_ab_payments_silver= df_ab_payments

df_ap_clients_silver = protection_silver(df_ap_clients,"SQLServer")
# à faire drop 
df_ap_contrats_silver = df_ap_contrats
df_ap_sinistres_silver = df_ap_sinistres
df_ap_payments_silver= df_ap_payments


# Tables Oracle
enregistrement_silver(df_ab_clients_silver,"workspace.silver.oracle_clients")
enregistrement_silver(df_ab_contrats_silver,"workspace.silver.oracle_contrats")
enregistrement_silver(df_ab_sinistres_silver,"workspace.silver.oracle_sinistres")
enregistrement_silver(df_ab_payments_silver,"workspace.silver.oracle_payments")

# Tables SQL Server
enregistrement_silver(df_ap_clients_silver,"workspace.silver.sqlserver_clients")
enregistrement_silver(df_ap_contrats_silver,"workspace.silver.sqlserver_contrats")
enregistrement_silver(df_ap_payments_silver,"workspace.silver.sqlserver_payments")
enregistrement_silver(df_ap_sinistres_silver,"workspace.silver.sqlserver_sinistres")


#####   ICI NORMALEMENT COMMENCE LA COUCHE GOLD ###################################



from SONAR_FONC_HARMONISATION_ET_UNIFICATION_CLIENTS import harmonize_clients



dim_clients = harmonize_clients(df_ab_clients,df_ap_clients)
print("dim_clients")
display(dim_clients)


# Étape 3 : Matrice de Doublons et Création de la Dimension Client

from SONAR_FONC_MATRICE_DOUBLONS import generer_Matrice_Doublons

# Algorithme d'identification des doublons (Matching exact sur Nom, Prénom, Date de Naissance ou Téléphone)
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, upper, initcap
'''
matched_clients,dim_clients,doublons_df,result_df = generer_Matrice_Doublons(dim_clients)

print("Matrice des doublons")

display(dim_clients.join(doublons_df, "MASTER_CLIENT_ID"))
'''

# Hypothèse : df_clients_initial est votre DataFrame propre d'origine
# Si vous n'avez que dim_clients, assurez-vous de réexécuter la cellule du dessus qui le génère initialement.

# res_matched, res_dim, res_doublons, res_df = generer_Matrice_Doublons(dim_clients)     ceci marche 
matched_clients, dim_clients, doublons_df,result_df =generer_Matrice_Doublons(dim_clients)

print("Matrice des doublons")
display(dim_clients.join(doublons_df, "MASTER_CLIENT_ID"))






# Protection GOLD

def protection_gold(df):
    
    salt = dbutils.secrets.get(scope="PROJET_BIG_DATA_STUDI", key="hash_salt")
    
    df = df.withColumns({"Email_Hached": F.sha2(F.concat(F.col("EMAIL"), F.lit(salt)), 256),

    "Nom_Hached": F.sha2(F.concat(F.col("NOM"), F.lit(salt)), 256),
    "Prenom_Hached": F.sha2(F.concat(F.col("PRENOM"), F.lit(salt)), 256),
    "Date_de_naissance_Hached": F.sha2(F.concat(F.col("DATE_NAISSANCE"), F.lit(salt)), 256),
    "Numero_fiscal_Hached": F.sha2(F.concat(F.col("NUMERO_FISCAL"), F.lit(salt)), 256),
    "Adresse_Hached": F.sha2(F.concat(F.col("ADRESSE"), F.lit(salt)), 256),
    "TELEPHONE_Hached": F.sha2(F.concat(F.col("TELEPHONE"), F.lit(salt)), 256)}).drop("NOM", "PRENOM","EMAIL","DATE_NAISSANCE", "NUMERO_FISCAL", "ADRESSE", "TELEPHONE")

    return df

print(" ------Matrice des doublons après chiffrement---------------")

dim_clients_protege= protection_gold(dim_clients)
#dim_clients_gold = dim_clients_protege.drop("NOM","DATE_NAISSANCE","PRENOM")
dim_clients_gold = dim_clients_protege
display(dim_clients_gold.join(doublons_df, "MASTER_CLIENT_ID"))

# Enregistrment GOLD Client


dim_clients_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.dim_clients_gold")


# Étape 4 : Tables de Faits Contrats et Sinistres et payments (Gold Layer)

from SONAR_FONC_UNIFICATION_ET_GOLD_CONTRAT import unify_and_map_contrats

# Unification Contrats

fact_contrats_gold = unify_and_map_contrats(df_ab_contrats, df_ap_contrats, dim_clients)

'''
facts_contrats = df_ab_contrats.select(
    col("AB_POLICY_NUMBER").cast("string").alias("CONTRAT_ID"),
    lit("ORACLE").alias("SOURCE_SYSTEM"),
    col("AB_CLIENT_ID").cast("string").alias("ORIGINAL_CLIENT_ID"),
    col("AB_TYPE_ASSURANCE").alias("TYPE_CONTRAT"),
    col("AB_STATUT_CONTRAT").alias("STATUT_CONTRAT"),
    col("AB_PRIME_ANNUELLE").alias("MONTANT_PRIME")
).union(
    df_ap_contrats.select(
        col("AP_CONTRACT_REF").cast("string").alias("CONTRAT_ID"),
        lit("SQL").alias("SOURCE_SYSTEM"),
        col("AP_USER_ID").cast("string").alias("ORIGINAL_CLIENT_ID"),
        col("AP_PRODUCT_CODE").alias("TYPE_CONTRAT"),
        col("AP_CONTRACT_STATE").alias("STATUT_CONTRAT"),
        col("AP_MONTHLY_PREMIUM").alias("MONTANT_PRIME")
    )
)
print("#------ Affichage des contrats --------")

display(facts_contrats)

# Mapping avec Master_Client_ID
fact_contrat_gold = facts_contrats.join(
    dim_clients,
    (facts_contrats.ORIGINAL_CLIENT_ID == dim_clients.ORIGINAL_ID) & 
    (facts_contrats.SOURCE_SYSTEM == dim_clients.SOURCE_SYSTEM)
).select("CONTRAT_ID", "MASTER_CLIENT_ID", "TYPE_CONTRAT", "MONTANT_PRIME", "STATUT_CONTRAT")

print("#------ Affichage des contrats  après mapping --------")
'''
display(fact_contrats_gold)
# faire une protection gold 

fact_contrats_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.fact_contrats")

'''
# Unification Sinistres
fact_sinistres_gold = df_ab_sinistres.select(
    col("AB_CLAIM_ID").cast("string").alias("SINISTRE_ID"),
    col("AB_POLICY_NUMBER").cast("string").alias("CONTRAT_ID"),
    col("AB_MONTANT_ESTIME").alias("MONTANT_INDEMNISE"),
     col("AB_STATUT_SINISTRE").alias("STATUT_SINISTRE")
).union(
    df_ap_sinistres.select(
        col("AP_SINISTRE_NUM").cast("string").alias("SINISTRE_ID"),
        col("AP_CONTRACT_REF").alias("CONTRAT_ID"),
        col("AP_ESTIMATED_AMOUNT").alias("MONTANT_INDEMNISE"),
        col("AP_CLAIM_STATUS").alias("STATUT_SINISTRE")

    )
)

fact_sinistres_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.fact_sinistres")
'''


# Unification Sinistres

from  SONAR_FONC_UNIFICATION_ET_GOLD_SINISTRE   import unify_and_map_sinistres

fact_sinistres_gold = unify_and_map_sinistres(df_ab_sinistres, df_ap_sinistres, fact_contrats_gold, table_name="gold.fact_sinistres")

''' ici l ancienne version qui marche 
fact_sinistres_gold = df_ab_sinistres.select(
    col("AB_CLAIM_ID").cast("string").alias("SINISTRE_ID"),
    col("AB_POLICY_NUMBER").cast("string").alias("CONTRAT_ID"),
    col("AB_MONTANT_ESTIME").alias("MONTANT_INDEMNISE"),
     col("AB_STATUT_SINISTRE").alias("STATUT_SINISTRE")
).union(
    df_ap_sinistres.select(
        col("AP_SINISTRE_NUM").cast("string").alias("SINISTRE_ID"),
        col("AP_CONTRACT_REF").alias("CONTRAT_ID"),
        col("AP_ESTIMATED_AMOUNT").alias("MONTANT_INDEMNISE"),
        col("AP_CLAIM_STATUS").alias("STATUT_SINISTRE")

    )
)

fact_sinistres_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.fact_sinistres")
# Mapping des sinistres avec la dim_clients pour obtenir MASTER_CLIENT_ID

fact_sinistres_gold_mapped = fact_sinistres_gold.join(
    fact_contrats_gold.select("CONTRAT_ID", "MASTER_CLIENT_ID"),
    "CONTRAT_ID",
    how="left"
).select(
    "SINISTRE_ID",
    "CONTRAT_ID",
    "MASTER_CLIENT_ID",
    "MONTANT_INDEMNISE",
    "STATUT_SINISTRE"
)

display(fact_sinistres_gold_mapped)

fact_sinistres_gold_mapped.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.fact_sinistres_mapped")


'''

# Unification payments

from  SONAR_FONC_UNIFICATION_ET_GOLD_PAYMENT   import unify_and_map_payments

fact_payments_gold_mapped = unify_and_map_payments(df_ab_payments, df_ap_payments, fact_contrats_gold, table_name="gold.fact_payments")
'''

fact_payments_gold = df_ab_payments.select(
    col("AB_PAYMENT_ID").cast("string").alias("PAYMENT_ID"),
    col("AB_POLICY_NUMBER").cast("string").alias("CONTRAT_ID"),
    col("AB_DATE_PAYMENT").alias("DATE_PAYMENT"),
    col("AB_MONTANT").alias("MONTANT_INDEMNISE"),
    col("AB_MODE_PAYMENT").alias("MODE_PAYMENT")
).union(
    df_ap_payments.select(
    col("AP_PAYMENT_REF").cast("string").alias("PAYMENT_ID"),
    col("AP_CONTRACT_REF").cast("string").alias("CONTRAT_ID"),
    col("AP_PAYMENT_DATETIME").alias("DATE_PAYMENT"),
    col("AP_AMOUNT_PAID").alias("MONTANT_INDEMNISE"),
    col("AP_PAYMENT_CHANNEL").alias("MODE_PAYMENT")
    )
)

fact_payments_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.fact_payments")


# Mapping des payments avec la dim_clients pour obtenir MASTER_CLIENT_ID

fact_payments_gold_mapped = fact_payments_gold.join(
    fact_contrats_gold.select("CONTRAT_ID", "MASTER_CLIENT_ID"),
    "CONTRAT_ID",
    how="left"
).select(
    "PAYMENT_ID",
    "CONTRAT_ID",
    "MASTER_CLIENT_ID",
    "DATE_PAYMENT",
    "MONTANT_INDEMNISE",
    "MODE_PAYMENT"
)

display(fact_payments_gold_mapped)

fact_payments_gold_mapped.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.fact_payments_mapped")

'''


#Dossier 3 (Suite) : SONAR_TESTs unitaires, Qualité & Dashboards

#Question 1 & 2 : SONAR_TESTs unitaires PySpark & SONAR_TEST Coverage
# Pour valider votre code (compatible avec les SONAR_TESTs unitaires PySpark) :

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
LEFT JOIN gold_fact_sinistres ON c.contrat_id = s.contrat_id;

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
df_ap_clients, df_ap_contrats, df_ap_sinistres, df_ap_payments = load_ap_data()

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
generer_matrice_doublons = generer_Matrice_Doublons

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
        load_ap_data()

    def test1_enregistrement_bronze(self):
        test_enregistrement_bronze()

    def test1_ajoutmétadonnées(self):
        test_ajoutmétadonnées()
# mettre ici le test de nettoyage
    def test1_enregistrement_silver(self):
       test_enregistrement_silver()
# test_harmonize_clients()


    def test1_generer_Matrice_Doublons(self):
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
    def SONAR_TEST1_generer_Matrice_Doublons(self):
      SONAR_TEST_generer_Matrice_Doublons()
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
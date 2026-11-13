# Étape 2 : Data Pipeline - Harmonisation & Unification (Silver Layer)

from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap, col, lower, upper, trim, concat_ws, levenshtein, when, lit, count,concat, sum as _sum, avg, round, to_date, regexp_extract, regexp_replace, rlike


from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
from datetime import datetime
from pyspark.sql.window import Window
import re


from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, DoubleType
from pyspark.sql import functions as F
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





# Nettoyage des noms 
#from pyspark.sql import functions as F
def clean_last_name(df,last_name_col):
  df = df.withColumn(
  last_name_col,
  F.upper(F.trim(F.col(last_name_col))))
  return df

# Nettoyage des prénoms

# 1. Définition de la fonction (elle prend un nom de colonne en chaîne de caractères)
def normalize_firstname(column_name):
    return F.upper(F.trim(F.col(column_name)))


# nettoyage des Emails

# 1. Définition de la fonction (elle prend un nom de colonne en chaîne de caractères)
def normalize_email(column_name):
    return F.lower(F.trim(F.col(column_name)))

# Nettoyage des dates 
def normalize_date (df,col_name):
  df= df.withColumn(col_name, F.try_to_date(col(col_name), "yyyy-MM-dd"))    # j'ai mis try_to_date au lieu de to_date
  return df 

# Nettoyage des numéros de Téléphone mais rajouter +33 


# Correction de la fonction normalize_phone_number (format international)
def normalize_phone_number(df, column_name):
    # Supprime tout sauf les chiffres et applique le format français
    return df.withColumn(column_name, concat(lit("+33"), regexp_replace(regexp_replace(col(column_name), r'[^0-9]', ''), r'^0', '') ))

# -------------------     NETTOYAGE  -------------------------------
def data_cleaning(df1, df2):


# Nettoyage des noms des clients ORACLE

 df1 = clean_last_name(df1,"AB_NOM")

# Nettoyage des noms des users SQLServer

 df2 = clean_last_name(df2,"AP_NOM")



# 2. Application  sur le DataFrame Oracle
 df1 = df1.withColumn("AB_PRENOM", normalize_firstname("AB_PRENOM"))

# 3. Application  sur le DataFrame SQL
 df2= df2.withColumn("AP_PRENOM", normalize_firstname("AP_PRENOM"))



# 2. Application  sur le DataFrame Oracle
 df1 = df1.withColumn("AB_EMAIL", normalize_email("AB_EMAIL"))

# 2. Application  sur le DataFrame SQL
 df2= df2.withColumn("AP_MAIL_ADDRESS", normalize_email("AP_MAIL_ADDRESS"))

                                                                                                            

#appliquée à Oracle
 df1= normalize_date(df1,"AB_DATE_NAISSANCE")
 # df1= df1.withColumn("AB_DATE_CREATION", F.try_to_timestamp(col("AB_DATE_CREATION"), "yyyy-MM-dd HH:mm:ss"))
 df1= df1.withColumn("AB_DATE_CREATION", F.try_to_timestamp(col("AB_DATE_CREATION"), F.lit("yyyy-MM-dd HH:mm:ss")))
 

#appliquée à SQL Server
 df2= normalize_date(df2,"AP_BIRTH_DATE")
 #df2= df2.withColumn("AP_DATE_CREATION", F.try_to_timestamp(col("AP_DATE_CREATION"), "yyyy-MM-dd HH:mm:ss"))
 df2= df2.withColumn("AP_DATE_CREATION", F.try_to_timestamp(col("AP_DATE_CREATION"), F.lit("yyyy-MM-dd HH:mm:ss")))


# Appliquer la fonction aux colonnes de téléphone
 df1 = normalize_phone_number(df1 , "AB_TELEPHONE")
 df2 = normalize_phone_number(df2 , "AP_PHONE_NUMBER")

 df1.show(truncate=False)
 df2.show(truncate=False)

 return df1,df2


# Étape 2 : Data Pipeline - Harmonisation & Unification (Silver Layer)

from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap, col, lower, upper, trim, concat_ws, levenshtein, when, lit, count, sum as _sum, avg, round, to_date
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
from datetime import datetime
from pyspark.sql.window import Window


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





# -------------------       NETTOYAGE  -------------------------------

 # Scinder en nom et prénom le full name de Assurplus
def split_full_name(df,col_last_name, col_first_name,col_full_name):

 df = df.withColumn(col_last_name, F.trim(F.split(F.col(col_full_name), " ").getItem(1))) \
             .withColumn(col_first_name, F.trim(F.split(F.col(col_full_name), " ").getItem(0)))
 return df

# df_ap_clients = split_full_name(df_ap_clients,"AP_NOM","AP_PRENOM","AP_FULL_NAME")  appelée dans le programme principal

# Nettoyage des noms 
from pyspark.sql import functions as F



# Nettoyage des noms des clients ORACLE

# appele du progarme principal df_ab_clients non connu ici 

df_ab_clients = clean_last_name(df_ab_clients,"AB_NOM")

# Nettoyage des noms des users SQLServer

df_ap_clients = clean_last_name(df_ap_clients,"AP_NOM")



# Nettoyage des prénoms



# 2. Application  sur le DataFrame Oracle
# df_ab_clients = df_ab_clients.withColumn("AB_PRENOM", normalize_firstname("AB_PRENOM"))

# 3. Application  sur le DataFrame SQL
#df_ap_clients= df_ap_clients.withColumn("AP_PRENOM", normalize_firstname("AP_PRENOM"))

# nettoyage des Emails

# 1. Définition de la fonction (elle prend un nom de colonne en chaîne de caractères)


# 2. Application  sur le DataFrame Oracle
#df_ab_clients = df_ab_clients.withColumn("AB_EMAIL", normalize_email("AB_EMAIL"))

# 2. Application  sur le DataFrame SQL
#df_ap_clients= df_ap_clients.withColumn("AP_MAIL_ADDRESS", normalize_email("AP_MAIL_ADDRESS"))

# Nettoyage des dates 


#appliquée à Oracle
#df_ab_clients= normalize_date(df_ab_clients,"AB_DATE_NAISSANCE")
#df_ab_clients= df_ab_clients.withColumn("AB_DATE_CREATION", to_timestamp(col("AB_DATE_CREATION"), "yyyy-MM-dd HH:mm:ss"))

#appliquée à SQL Server
#df_ap_clients= normalize_date(df_ap_clients,"AP_BIRTH_DATE")
#df_ap_clients= df_ap_clients.withColumn("AP_DATE_CREATION", to_timestamp(col("AP_DATE_CREATION"), "yyyy-MM-dd HH:mm:ss"))


# Nettoyage des numéros de Téléphone mais rajouter +33 



# Appliquer la fonction aux colonnes de téléphone
#df_ab_clients = normalize_phone_number(df_ab_clients, "AB_TELEPHONE")
#df_ap_clients = normalize_phone_number(df_ap_clients, "AP_PHONE_NUMBER")
#display(df_ab_clients)
#display(df_ap_clients)

"""
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
        .withColumn({"address_column": trim(regexp_replace(col(address_column), r'\s+', ' ')),
        "postal_code_column": regexp_replace(col(postal_code_column), r'\D', ''),
        "city_column": upper(trim(col(city_column)))}))

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
  df= df.withColumn(col_name, to_date(col(col_name), "yyyy-MM-dd"))
  return df 

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



# -------------------       NETTOYAGE  -------------------------------
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
 df1= df1.withColumn("AB_DATE_CREATION", to_timestamp(col("AB_DATE_CREATION"), "yyyy-MM-dd HH:mm:ss"))

#appliquée à SQL Server
 df2= normalize_date(df2,"AP_BIRTH_DATE")
 df2= df2.withColumn("AP_DATE_CREATION", to_timestamp(col("AP_DATE_CREATION"), "yyyy-MM-dd HH:mm:ss"))



# Appliquer la fonction aux colonnes de téléphone
 df1 = normalize_phone_number(df1, "AB_TELEPHONE")
 df2 = normalize_phone_number(df2, "AP_PHONE_NUMBER")

 display(df1)
 display(df2)

 return df1, df2




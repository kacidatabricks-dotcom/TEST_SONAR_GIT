"""
# Étape 2 : Data Pipeline - Harmonisation & Unification (Silver Layer)

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

#   IL FAUT CREER  UNE FONCTION DE TEST DE NETTOYAGE


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
#df_ap_clients = split_full_name(df_ap_clients,"AP_NOM","AP_PRENOM","AP_FULL_NAME")

# Nettoyage des noms 
from pyspark.sql import functions as F
def clean_last_name(df,last_name_col):
  df = df.withColumn(
  last_name_col,
  F.upper(F.trim(F.col(last_name_col))))
  return df


# Nettoyage des noms des clients ORACLE


#df_ab_clients = clean_last_name(df_ab_clients,"AB_NOM")

# Nettoyage des noms des users SQLServer

#df_ap_clients = clean_last_name(df_ap_clients,"AP_NOM")



# Nettoyage des prénoms

# 1. Définition de la fonction (elle prend un nom de colonne en chaîne de caractères)
def normalize_firstname(column_name):
    return F.upper(F.trim(F.col(column_name)))

# 2. Application  sur le DataFrame Oracle
#df_ab_clients = df_ab_clients.withColumn("AB_PRENOM", normalize_firstname("AB_PRENOM"))

# 3. Application  sur le DataFrame SQL
#df_ap_clients= df_ap_clients.withColumn("AP_PRENOM", normalize_firstname("AP_PRENOM"))

# nettoyage des Emails

# 1. Définition de la fonction (elle prend un nom de colonne en chaîne de caractères)
def normalize_email(column_name):
    return F.lower(F.trim(F.col(column_name)))

# 2. Application  sur le DataFrame Oracle
#df_ab_clients = df_ab_clients.withColumn("AB_EMAIL", normalize_email("AB_EMAIL"))

# 2. Application  sur le DataFrame SQL
#df_ap_clients= df_ap_clients.withColumn("AP_MAIL_ADDRESS", normalize_email("AP_MAIL_ADDRESS"))

# Nettoyage des dates 
def normalize_date (df,col_name):
  # coalesce avec deux formats : gère les dates simples ("1985-03-15") et les horodatages ("2020-01-15 10:30:00")
  df = df.withColumn(col_name, coalesce(try_to_date(col(col_name), "yyyy-MM-dd"), try_to_date(col(col_name), "yyyy-MM-dd HH:mm:ss")))
  return df

#appliquée à Oracle
#df_ab_clients= normalize_date(df_ab_clients,"AB_DATE_NAISSANCE")
#df_ab_clients= df_ab_clients.withColumn("AB_DATE_CREATION", to_timestamp(col("AB_DATE_CREATION"), "yyyy-MM-dd HH:mm:ss"))

#appliquée à SQL Server
#df_ap_clients= normalize_date(df_ap_clients,"AP_BIRTH_DATE")
#df_ap_clients= df_ap_clients.withColumn("AP_DATE_CREATION", to_timestamp(col("AP_DATE_CREATION"), "yyyy-MM-dd HH:mm:ss"))


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
#df_ab_clients = normalize_phone_number(df_ab_clients, "AB_TELEPHONE")
#df_ap_clients = normalize_phone_number(df_ap_clients, "AP_PHONE_NUMBER")

#display(df_ab_clients)
#display(df_ap_clients)


# Fonction de test de nettoyage utilisant les fonctions définies et le fichier SRC/Fonc_nettoyage_des_deux_bases
def test_nettoyage_des_deux_bases(df_raw):
    """
    Teste le pipeline de nettoyage des données sur un DataFrame brut.
    Applique toutes les fonctions de nettoyage définies.
    """
    df = df_raw
    # Exemple d'application des étapes de nettoyage
    df = handle_nulls(df, "EMAIL", default_value="inconnu@domain.com", strategy="fill")
    df = clean_address(df, "ADRESSE", "CODE_POSTAL", "VILLE")
    df = clean_last_name(df, "NOM")
    df = df.withColumn("PRENOM", normalize_firstname("PRENOM"))
    df = df.withColumn("EMAIL", normalize_email("EMAIL"))
    df = normalize_date(df, "DATE_NAISSANCE")
    df = normalize_date(df, "DATE_CREATION")
    df = normalize_phone_number(df, "TELEPHONE")
    df = df.withColumn("SOURCE_FILE", F.lit(source_file))
    display(df)
    return df

# Exemple d'utilisation :  INTERESSANT
#test_nettoiage_des_deux_bases(df_clients)



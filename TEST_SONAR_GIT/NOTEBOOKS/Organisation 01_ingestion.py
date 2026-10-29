# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# %md#
# 01 - Ingestion
 #En Free Edition, les CSV simulent les exports Oracle et SQL Server.

from pyspark.sql import functions as F

# BASE = "/Volumes/<catalog>/<schema>/data"
path_ab_clients = "/Workspace/Users/kacidatabricks@gmail.com/Projet Assurance/abassurance_databricks_bloc3_kit/DATA/SAMPLE/ABASSURANCE/clients.csv"
path_ap_users = "/Workspace/Users/kacidatabricks@gmail.com/Projet Assurance/abassurance_databricks_bloc3_kit/DATA/SAMPLE/APASSURANCE/users.csv"
ab_client = spark.read.option("header", True).option("inferSchema", True).csv(path_ab_clients)
ap_users = spark.read.option("header", True).option("inferSchema", True).csv(path_ap_users)
# ab_client = spark.read.option("header", True).option("inferSchema", True).csv(f"{BASE}/abassurance/clients.csv")
# ap_users = spark.read.option("header", True).option("inferSchema", True).csv(f"{BASE}/assureplus/users.csv")

display(ab_client.limit(20))
display(ap_users.limit(20))

# Changer le format en majuscule des noms des clients Ab


ab_client = ab_client.withColumn(
"AB_NOM",
F.upper(F.trim(F.col("AB_NOM")))
)
# isplay(ab_client.limit(20))


# Changer le format en majuscule des prénoms  des clients Ab


ab_client = ab_client.withColumn(
"AB_PRENOM",
F.upper(F.trim(F.col("AB_PRENOM")))
)
# display(ab_client.limit(20))

# Changer le format en majuscule des FULL NAME des Users AP


ap_users = ap_users.withColumn(
"AP_FULL_NAME",
F.upper(F.trim(F.col("AP_FULL_NAME")))
)
 # display(ap_users.limit(20))

 # Scinder en nom et prénom le full name de Assurplus

ap_users = ap_users.withColumn("AP_NOM", F.trim(F.split(F.col("AP_FULL_NAME"), " ").getItem(1))) \
             .withColumn("AP_PRENOM", F.trim(F.split(F.col("AP_FULL_NAME"), " ").getItem(0)))

# ap_users.show()
 

# Changer le format en majuscule des numéros de téléphone  des Users AP


ap_users = ap_users.withColumn(
"AP_PHONE_NUMBER",
F.upper(F.trim(F.col("AP_PHONE_NUMBER")))
)


# Changer le format des AP MAIl ADRESS  des clients AP


ap_users = ap_users.withColumn(
"AP_MAIL_ADDRESS",
F.upper(F.trim(F.col("AP_MAIL_ADDRESS")))
)
# display(ap_users.limit(20))





# Changer le format des Ap Phone numbers des Users AP


ap_users = ap_users.withColumn(
"AP_PHONE_NUMBER",
F.upper(F.trim(F.col("AP_PHONE_NUMBER")))
)
 

# Changer le format en majuscule des noms des clients AB


ab_client = ab_client.withColumn(
"AB_EMAIL",F.upper(F.trim(F.col("AB_EMAIL")))
)



# Changer le format des Ap Phone numbers des Users AB


ab_client = ab_client.withColumn(
"AB_TELEPHONE",
F.upper(F.trim(F.col("AB_TELEPHONE"))))

# display( ab_client.limit(20))

# display( ap_users.limit(20))


# FORMAT TELEPHONE Ab Assurances

# from pyspark.sql import functions as F

# 1. Nettoyer pour garder uniquement les chiffres
# Supprime tout ce qui n'est pas un chiffre (espaces, points, tirets, +)
ab_client_clean = ab_client.withColumn("digits", F.regexp_replace(F.col("AB_TELEPHONE"), r"\D", ""))

# 2. Gérer le 0 initial français et formater par blocs de 2 chiffres
# Si le numéro commence par 33, on le garde ; si par 0, on le remplace par 33
ab_client_normalized = ab_client_clean.withColumn(
    "intl_digits",
    F.when(F.col("digits").startswith("0"), F.concat(F.lit("33"), F.substring(F.col("digits"), 2, 20)))
     .otherwise(F.col("digits"))
)

# 3. Appliquer le format +33 XX XX XX XX...
ab_client_formatted = ab_client_normalized.withColumn(
    "AB_TELEPHONE_format",
    F.regexp_replace(F.col("intl_digits"), r"(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})", "+$1 $2 $3 $4 $5")
)

ab_client_formatted.select("AB_TELEPHONE", "AB_TELEPHONE_format").show(truncate=False)

 #%run ./FormattélephoneAssurPlus.py


 #   ---------------------------------------------
 # FORMAT TELEPHONE AssurPlus

# from pyspark.sql import functions as F

# 1. Nettoyer pour garder uniquement les chiffres
# Supprime tout ce qui n'est pas un chiffre (espaces, points, tirets, +)
ap_users_clean = ap_users.withColumn("digits", F.regexp_replace(F.col("AP_PHONE_NUMBER"), r"\D", ""))

# 2. Gérer le 0 initial français et formater par blocs de 2 chiffres
# Si le numéro commence par 33, on le garde ; si par 0, on le remplace par 33
ap_users_normalized = ap_users_clean.withColumn(
    "intl_digits",
    F.when(F.col("digits").startswith("0"), F.concat(F.lit("33"), F.substring(F.col("digits"), 2, 20)))
     .otherwise(F.col("digits"))
)

# 3. Appliquer le format +33 XX XX XX XX...
ap_users_formatted = ap_users_normalized.withColumn(
    "AP_PHONE_NUMBER_format",
    F.regexp_replace(F.col("intl_digits"), r"(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})", "+$1 $2 $3 $4 $5")
)

ap_users_formatted.select("AP_PHONE_NUMBER", "AP_PHONE_NUMBER_format").show(truncate=False)

# Normalisation des dates ab_client

ab_client = ab_client.withColumn(
"AB_DATE_CREATION",
F.to_date(
"AB_DATE_CREATION",
"yyyy-MM-dd"
)
)

ab_client = ab_client.withColumn(
"AB_DATE_NAISSANCE",
F.to_date(
"AB_DATE_NAISSANCE",
"yyyy-MM-dd"
)
)

# Normalisation des dates ap_users

ap_users = ap_users.withColumn(
"AP_CREATED_AT",
F.to_date(
"AP_CREATED_AT",
"yyyy-MM-dd"
)
)

ap_users = ap_users.withColumn(
"AP_BIRTH_DATE",
F.to_date(
"AP_BIRTH_DATE",
"yyyy-MM-dd"
)
)


display(ap_users.limit(20))
display(ab_client.limit(20))











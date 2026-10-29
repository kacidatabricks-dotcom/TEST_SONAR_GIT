# FORMAT TELEPHONE AssurPlus

# from pyspark.sql import functions as F

# 1. Nettoyer pour garder uniquement les chiffres
# Supprime tout ce qui n'est pas un chiffre (espaces, points, tirets, +)
ab_client_clean = ab_client.withColumn("digits", F.regexp_replace(F.col("AP_PHONE_NUMBER"), r"\D", ""))

# 2. Gérer le 0 initial français et formater par blocs de 2 chiffres
# Si le numéro commence par 33, on le garde ; si par 0, on le remplace par 33
ab_client_normalized = ab_client_clean.withColumn(
    "intl_digits",
    F.when(F.col("digits").startswith("0"), F.concat(F.lit("33"), F.substring(F.col("digits"), 2, 20)))
     .otherwise(F.col("digits"))
)

# 3. Appliquer le format +33 XX XX XX XX...
ab_client_formatted = ab_client_normalized.withColumn(
    "AP_PHONE_NUMBER_format",
    F.regexp_replace(F.col("intl_digits"), r"(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})", "+$1 $2 $3 $4 $5")
)

ab_client_formatted.select("AP_PHONE_NUMBER", "AP_PHONE_NUMBER_format").show(truncate=False)


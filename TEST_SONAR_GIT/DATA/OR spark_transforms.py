from pyspark.sql import functions as F

def clean_client_df(df):
    if "email" in df.columns:
        df = df.withColumn("email", F.lower(F.trim("email")))
    if "telephone" in df.columns:
        df = df.withColumn(
            "telephone",
            F.regexp_replace(F.trim("telephone"), r"[^0-9+]", "")
        )
    if "nom" in df.columns:
        df = df.withColumn("nom", F.initcap(F.trim("nom")))
    if "prenom" in df.columns:
        df = df.withColumn("prenom", F.initcap(F.trim("prenom")))
    if "date_naissance" in df.columns:
        df = df.withColumn("date_naissance", F.to_date("date_naissance"))
    return df

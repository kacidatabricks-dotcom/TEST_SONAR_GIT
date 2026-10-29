# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Data Quality + Matching
# MAGIC Exemple pédagogique. Sur de gros volumes, utiliser des blocking keys et éviter un cross join global.

from pyspark.sql import functions as F

ab = spark.table("silver_ab_client").alias("ab")
ap = spark.table("silver_ap_client").alias("ap")

# Exemple de blocking : même email.
candidate = (
    ab.join(
        ap,
        F.col("ab.email").isNotNull()
        & (F.col("ab.email") == F.col("ap.email")),
        "inner"
    )
)

matches = (
    candidate
    .withColumn(
        "same_email",
        (F.col("ab.email") == F.col("ap.email")).cast("int")
    )
    .withColumn(
        "same_phone",
        (F.col("ab.telephone") == F.col("ap.telephone")).cast("int")
    )
    .withColumn(
        "same_name",
        (
            (F.lower(F.col("ab.nom")) == F.lower(F.col("ap.nom")))
            &
            (F.lower(F.col("ab.prenom")) == F.lower(F.col("ap.prenom")))
        ).cast("int")
    )
    .withColumn(
        "same_birth_date",
        (F.col("ab.date_naissance") == F.col("ap.date_naissance")).cast("int")
    )
    .withColumn(
        "match_score",
        F.col("same_email") * 50
        + F.col("same_phone") * 30
        + F.col("same_name") * 10
        + F.col("same_birth_date") * 20
    )
    .withColumn(
        "decision",
        F.when(F.col("match_score") >= 80, "DOUBLON")
         .when(F.col("match_score") >= 50, "A_VERIFIER")
         .otherwise("DIFFERENT")
    )
)

matches.write.format("delta").mode("overwrite").saveAsTable("gold_duplicate_matrix")

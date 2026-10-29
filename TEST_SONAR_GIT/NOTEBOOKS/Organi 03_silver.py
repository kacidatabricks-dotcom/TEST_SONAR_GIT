# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Silver
# MAGIC Nettoyage + mapping.

from pyspark.sql import functions as F

ab = spark.table("bronze_ab_client")

silver_ab = (
    ab
    .withColumnRenamed("AB_CLIENT_ID", "source_client_id")
    .withColumnRenamed("AB_NOM", "nom")
    .withColumnRenamed("AB_PRENOM", "prenom")
    .withColumnRenamed("AB_DATE_NAISSANCE", "date_naissance")
    .withColumnRenamed("AB_EMAIL", "email")
    .withColumnRenamed("AB_TELEPHONE", "telephone")
    .withColumnRenamed("AB_ADRESSE", "adresse")
    .withColumnRenamed("AB_CODE_POSTAL", "code_postal")
    .withColumnRenamed("AB_NUM_FISCAL", "numero_fiscal")
    .withColumnRenamed("AB_DATE_CREATION", "date_creation")
    .withColumnRenamed("AB_STATUT_CLIENT", "statut_client")
    .withColumn("source_system", F.lit("ABASSURANCE"))
    .withColumn("email", F.lower(F.trim("email")))
    .withColumn("telephone", F.regexp_replace(F.trim("telephone"), r"[^0-9+]", ""))
    .withColumn("nom", F.initcap(F.trim("nom")))
    .withColumn("prenom", F.initcap(F.trim("prenom")))
    .withColumn("date_naissance", F.to_date("date_naissance"))
)

silver_ab.write.format("delta").mode("overwrite").saveAsTable("silver_ab_client")

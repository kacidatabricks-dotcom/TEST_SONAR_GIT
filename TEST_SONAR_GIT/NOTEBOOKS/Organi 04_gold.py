# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Gold
# MAGIC Modèle métier et statistiques.

from pyspark.sql import functions as F

silver_ab = spark.table("silver_ab_client")

gold_client = (
    silver_ab
    .withColumn("client_id", F.concat(F.lit("AB-"), F.col("source_client_id").cast("string")))
)

gold_client.write.format("delta").mode("overwrite").saveAsTable("gold_client")

spark.sql("""
CREATE OR REPLACE VIEW gold_client_stats AS
SELECT source_system, statut_client, COUNT(*) AS nb_clients
FROM gold_client
GROUP BY source_system, statut_client
""")

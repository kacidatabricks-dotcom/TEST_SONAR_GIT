# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# %md
# 02 - Bronze

ab_client.write.format("delta").mode("overwrite").saveAsTable("bronze_ab_client")
ap_users.write.format("delta").mode("overwrite").saveAsTable("bronze_ap_users")
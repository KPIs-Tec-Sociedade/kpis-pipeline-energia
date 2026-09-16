# Databricks notebook source
# COMMAND ----------
dbutils.widgets.text("catalog", "workspace", "Catalog Name")
catalog = dbutils.widgets.get("catalog")

spark.sql(f"USE CATALOG {catalog}")
print(f"Camada Bronze inicializada no catalogo: {catalog}")
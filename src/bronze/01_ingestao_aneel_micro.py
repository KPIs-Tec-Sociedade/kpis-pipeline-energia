# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
import pandas as pd
from pyspark.sql.functions import current_timestamp

# 1. A URL pública da ANEEL para Geração Distribuída (Vem "zipado")
url_aneel = "https://dadosabertos.aneel.gov.br/dataset/5e0fafd2-21b9-4d5b-b622-40438d40aba2/resource/b1bd71e7-d0ad-4214-9053-cbd58e9564a7/download/empreendimento-geracao-distribuida.zip"

print("Baixando e descompactando os dados da ANEEL direto para a memória (isso pode levar um minutinho)...")

# 2. O Pandas é inteligente o suficiente para ler um ZIP via URL
# encoding='latin1' é essencial para não quebrar a acentuação de cidades brasileiras
pdf = pd.read_csv(url_aneel, sep=';', compression='zip', encoding='latin1')

print("Convertendo para o motor do Spark...")
# 3. Convertendo para Spark (se o Pandas alertar sobre tipos mistos de dados, o Spark ignora e cria o schema)
df_raw = spark.createDataFrame(pdf.astype(str)) # Convertendo tudo para texto temporariamente na Bronze

# 4. Adicionando a data de ingestão
df_bronze = df_raw.withColumn("ingestion_date", current_timestamp())

# 5. Criando o banco
spark.sql("CREATE DATABASE IF NOT EXISTS projeto_energia")

print("Salvando os dados na Camada Bronze (Formato Delta)...")
(df_bronze.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("projeto_energia.bronze_geracao_distribuida"))

print("Ingestão da Bronze finalizada com sucesso! Veja as colunas de cidade e CEP:")
display(df_bronze)
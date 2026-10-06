# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
import os
import urllib.request
import zipfile
import shutil
from pyspark.sql.functions import current_timestamp

# 1. Configurar caminhos no disco temporário do cluster
url_aneel = "https://dadosabertos.aneel.gov.br/dataset/5e0fafd2-21b9-4d5b-b622-40438d40aba2/resource/b1bd71e7-d0ad-4214-9053-cbd58e9564a7/download/empreendimento-geracao-distribuida.zip"
diretorio_atual = os.getcwd()
caminho_zip = os.path.join(diretorio_atual, "aneel_temp.zip")
pasta_extracao = os.path.join(diretorio_atual, "aneel_csv_temp")

print("1. A transferir a atualização direto do portal da ANEEL...")
urllib.request.urlretrieve(url_aneel, caminho_zip)

print("2. A descompactar no servidor da nuvem...")
with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
    zip_ref.extractall(pasta_extracao)

# Encontrar o nome do ficheiro CSV extraído
nome_csv = [arq for arq in os.listdir(pasta_extracao) if arq.endswith('.csv')][0]
caminho_csv = os.path.join(pasta_extracao, nome_csv)

print("3. A ler os dados brutos com Spark (sem bloquear a memória!)...")
df_raw = (spark.read.format("csv")
          .option("header", "true")
          .option("sep", ";")
          .option("encoding", "utf-8") # AQUI ESTÁ A CORREÇÃO DE ACENTUAÇÃO!
          .option("inferSchema", "false") # Tudo como texto na Bronze
          .load(f"file:{caminho_csv}"))

print("4. A adicionar a data de ingestão...")
df_bronze = df_raw.withColumn("ingestion_date", current_timestamp())

print("5. A guardar na Tabela Delta (Camada Bronze Completa)...")
spark.sql("CREATE DATABASE IF NOT EXISTS projeto_energia")

(df_bronze.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("projeto_energia.bronze_geracao_distribuida"))

print("6. A limpar os ficheiros temporários do servidor...")
# Garantimos que o Workspace não fica cheio de ficheiros pesados
if os.path.exists(caminho_zip):
    os.remove(caminho_zip)
if os.path.exists(pasta_extracao):
    shutil.rmtree(pasta_extracao)

print("✅ Ingestão da Bronze finalizada com sucesso e com a formatação correta!")
display(df_bronze.limit(10))
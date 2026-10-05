# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ### Link fonte de dados: https://dados.ons.org.br/dataset/carga-energia$0
# MAGIC
# MAGIC ONS: Organização nacional do sistema elétrico
# MAGIC
# MAGIC Descrição: Dados de carga por subsistema em base diária, medida em MWmed.
# MAGIC
# MAGIC Até fevereiro/2021, os dados representam a carga atendida por usinas despachadas e/ou programadas pelo ONS, com base em dados recebidos pelo Sistema de Supervisão e Controle do ONS. Entre março/2021 e abril/23, os dados representam a carga atendida por usinas despachadas e/ou programadas pelo ONS, com base em dados recebidos pelo Sistema de Supervisão e Controle do ONS, mais a previsão de geração de usinas não despachadas pelo ONS. A partir de 29/04/2023, além dos dados anteriormente considerados, passou a ser incorporado o valor estimado da micro e minigeração distribuída (MMGD), com base em dados meteorológicos previstos.
# MAGIC
# MAGIC ### Dicionário
# MAGIC
# MAGIC
# MAGIC             titulo: DicionarioDados_Carga_Energia_Diaria,
# MAGIC             dicionario_simplificado: 
# MAGIC         
# MAGIC             "descricao": "Código do Subsistema",
# MAGIC             "codigo": "id_subsistema" ,
# MAGIC         
# MAGIC             "descricao": "Nome do Subsistema",
# MAGIC             "codigo": "nom_subsistema",
# MAGIC         
# MAGIC             "descricao": "Data de referência",
# MAGIC             "codigo": "din_instante",
# MAGIC         
# MAGIC             "descricao": "Valor da Carga de Energia, em MWmed",
# MAGIC             "codigo": "val_cargaenergiamwmed"
# MAGIC     
# MAGIC

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import current_timestamp

# 1. Definir a URL pública do ONS
url = "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/carga_energia_di/CARGA_ENERGIA_2026.csv"

# 2. Lemos direto da URL usando Pandas (ele lida perfeitamente com links http)
pdf = pd.read_csv(url, sep=';')

# 3. Convertendo o DataFrame Pandas para um DataFrame PySpark
df_raw = spark.createDataFrame(pdf)

# 4. Adicionando a data de ingestão (Metadado crucial da camada Bronze)
df_bronze = df_raw.withColumn("ingestion_date", current_timestamp())

# 5. Criando o banco lógico e salvando a tabela Delta
# Vi na sua imagem que você tentou salvar como "projeto_energia.ing", 
# mas vamos usar "bronze_carga" para manter o padrão da arquitetura!
spark.sql("CREATE DATABASE IF NOT EXISTS projeto_energia")

(df_bronze.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("projeto_energia.bronze_carga"))

print("Ingestão da Bronze finalizada com sucesso!")

# 6. Usando o DISPLAY para visualização interativa
display(df_bronze)
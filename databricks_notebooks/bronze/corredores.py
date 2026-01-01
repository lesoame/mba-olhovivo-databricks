# Databricks notebook source
# MAGIC %md
# MAGIC A categoria Corredores possibilita uma consulta que retorna todos os corredores inteligentes da cidade de São Paulo.

# COMMAND ----------

# ============================================================
# Ingestão incremental da API Olho Vivo - Corredores
# Frequência sugerida: Diária ou Semanal (Cadastro estático)
# Camada: Bronze (Azure Standard / Hive Metastore)
# ============================================================

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from datetime import datetime

# --- 1. CONFIGURAÇÕES ---
storage_account = "datalakeprojmba" 
container = "datalakeprojmba" 
meu_token = "6af34791de77cb4fe108edab6287fa51829405c1781e99e0e2d81cd0114cf3e7"

# Caminhos
base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
caminho_landing = f"{base_path}/olhovivo/bronze/landing_zone/corredores"
tabela_full_name = "olhovivo_bronze.corredores"
base_url = "https://api.olhovivo.sptrans.com.br/v2.1"

# --- LIMPEZA DE AMBIENTE (DEV) ---
caminho_tabela_fisico = f"{base_path}/olhovivo/bronze/corredores"
# Descomente para resetar tabela
dbutils.fs.rm(caminho_tabela_fisico, True)
spark.sql(f"DROP TABLE IF EXISTS {tabela_full_name}")
print(f"🧹 Faxina completa em: {caminho_tabela_fisico}")


# --- 2. DEFINIÇÃO EXPLÍCITA DO SCHEMA ---
schema_corredor = StructType([
    StructField("cc", IntegerType(), True), 
    StructField("nc", StringType(), True)   
])

# --- 3. AUTENTICAÇÃO ---
session = requests.Session()
auth = session.post(f"{base_url}/Login/Autenticar?token={meu_token}")

if auth.text.lower() != "true":
    raise Exception(f"❌ Falha na autenticação: {auth.text}")
print("✅ Autenticado.")

# --- 4. COLETA DE DADOS ---
print("🔍 Consultando lista mestre de corredores...")
try:
    resp = session.get(f"{base_url}/Corredor", timeout=10)
    
    if resp.status_code == 200:
        dados = resp.json()
        print(f"🔹 Registros retornados: {len(dados)}")
        
        # Cria DataFrame aplicando o Schema Forte
        df = spark.createDataFrame(dados, schema=schema_corredor)
        
    else:
        raise Exception(f"Erro na API: HTTP {resp.status_code}")

except Exception as e:
    raise Exception(f"❌ Erro fatal na coleta: {e}")

# --- 5. PROCESSAMENTO E GRAVAÇÃO ---
if df.count() > 0:
    # Enriquecimento
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    df = df.withColumn("dt_ingestao", current_timestamp()) \
           .withColumn("arquivo_origem", lit(f"corredores_{timestamp_str}.json"))

    # A. Salva JSON Raw
    caminho_arquivo_json = f"{caminho_landing}/corredores_{timestamp_str}.json"
    df.write.mode("overwrite").json(caminho_arquivo_json)
    print(f"🗂️ JSON salvo em: {caminho_arquivo_json}")

    # B. Salva Delta (Bronze)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tabela_full_name} (
            cc INT COMMENT 'Código identificador do corredor.',
            nc STRING COMMENT 'Nome do corredor.',
            dt_ingestao TIMESTAMP COMMENT 'Timestamp da ingestão dos dados no ambiente Bronze.',
            arquivo_origem STRING COMMENT 'Nome do arquivo JSON que originou os dados.'
        ) USING DELTA
    """)

    # Append
    (df.write
       .format("delta")
       .mode("append")
       .option("mergeSchema", "true") 
       .saveAsTable(tabela_full_name)
    )

    print(f"✅ Sucesso! Tabela '{tabela_full_name}' atualizada.")

    # C. Metadados
    descricao = "Tabela Bronze: Cadastro Mestre de Corredores Inteligentes."
    spark.sql(f"COMMENT ON TABLE {tabela_full_name} IS '{descricao}'")
    
    display(df)

else:
    print("❌ Nenhum dado encontrado.")
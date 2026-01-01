# Databricks notebook source
# MAGIC %md
# MAGIC Realiza uma busca por todos os pontos de parada atendidos por uma determinada linha.

# COMMAND ----------

# ============================================================
# Ingestão incremental da API Olho Vivo - Paradas por Linha
# Frequência: Semanal (Processo longo e pesado)
# Camada: Bronze (Azure Standard / Hive Metastore)
# ============================================================

import requests
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from datetime import datetime

# --- 1. CONFIGURAÇÕES ---
storage_account = "datalakeprojmba" 
container = "datalakeprojmba" 
meu_token = "6af34791de77cb4fe108edab6287fa51829405c1781e99e0e2d81cd0114cf3e7"

# Caminhos
base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
caminho_landing = f"{base_path}/olhovivo/bronze/landing_zone/paradas_por_linha"
tabela_full_name = "olhovivo_bronze.paradas_por_linha"
base_url = "https://api.olhovivo.sptrans.com.br/v2.1"

# --- LIMPEZA DE AMBIENTE (DEV) ---
caminho_tabela_fisico = f"{base_path}/olhovivo/bronze/paradas_por_linha"
# dbutils.fs.rm(caminho_tabela_fisico, True)
# spark.sql(f"DROP TABLE IF EXISTS {tabela_full_name}")


# --- 2. DEFINIÇÃO DO SCHEMA ---
schema_paradas_linha = StructType([
    StructField("cp", IntegerType(), True), 
    StructField("np", StringType(), True),  
    StructField("ed", StringType(), True),  
    StructField("py", DoubleType(), True),  
    StructField("px", DoubleType(), True),  
    StructField("cod_linha", IntegerType(), True) # <--- Chave Estrangeira
])

# --- 3. AUTENTICAÇÃO ---
session = requests.Session()
auth = session.post(f"{base_url}/Login/Autenticar?token={meu_token}")

if auth.text.lower() != "true":
    raise Exception(f"❌ Falha na autenticação: {auth.text}")
print("✅ Autenticado.")


# --- 4. PREPARAÇÃO: LER AS LINHAS DO DATA LAKE ---
# Em vez de ler JSON, lemos a Tabela Bronze que já criamos. Isso é Data Lineage!
print("🔍 Lendo tabela de linhas (olhovivo_bronze.buscar_linhas)...")

try:
    df_linhas = spark.table("olhovivo_bronze.buscar_linhas")
    
    # Pegamos apenas os códigos distintos
    # IMPORTANTE: Coloquei limit(20) para testes rápidos. 
    # Para produção, remova o .limit(20)
    lista_linhas = df_linhas.select("cl").distinct().collect()
    
    codigos_linha = [row['cl'] for row in lista_linhas]
    print(f"🔹 Fila de processamento: {len(codigos_linha)} linhas para consultar.")
    
except Exception as e:
    raise Exception(f"❌ Erro ao ler tabela de linhas. Rode o notebook de Linhas primeiro! Erro: {e}")


# --- 5. COLETA DE DADOS (LOOP) ---
lista_dfs = []
total_consultas = 0
consultas_sucesso = 0

for codigo in codigos_linha:
    total_consultas += 1
    
    try:
        resp = session.get(
            f"{base_url}/Parada/BuscarParadasPorLinha",
            params={"codigoLinha": codigo},
            timeout=15
        )

        if resp.status_code == 200:
            dados = resp.json()
            
            if len(dados) > 0:
                consultas_sucesso += 1
                
                # Injetamos o código da linha no dicionário antes de criar o DF
                for p in dados:
                    p['cod_linha'] = codigo
                
                df_temp = spark.createDataFrame(dados, schema=schema_paradas_linha)
                lista_dfs.append(df_temp)
                print(f"   ✅ Linha {codigo}: {len(dados)} paradas.")
            else:
                print(f"   ⚠️ Linha {codigo}: 0 paradas.")
        else:
            print(f"   ⚠️ Erro HTTP {resp.status_code} na linha {codigo}")

    except Exception as e:
        print(f"   ❌ Erro técnico linha {codigo}: {e}")
    
    time.sleep(0.1) # Pausa curta

print("="*40)
print(f"Resumo: {consultas_sucesso} sucessos.")
print("="*40)

# --- 6. PROCESSAMENTO E GRAVAÇÃO ---
if lista_dfs:
    # União
    df = lista_dfs[0]
    for df_temp in lista_dfs[1:]:
        df = df.union(df_temp)

    # Enriquecimento
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    df = df.withColumn("dt_ingestao", current_timestamp()) \
           .withColumn("arquivo_origem", lit(f"paradas_linha_{timestamp_str}.json"))
    
    print(f"\n✅ Total registros: {df.count()}")

    # A. Salva JSON Raw
    caminho_arquivo_json = f"{caminho_landing}/paradas_linha_{timestamp_str}.json"
    df.write.mode("overwrite").json(caminho_arquivo_json)
    print(f"🗂️ JSON salvo em: {caminho_arquivo_json}")

    # B. Salva Delta (Bronze)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tabela_full_name} (
            cp INT COMMENT 'Código identificador da parada.',
            np STRING COMMENT 'Nome da parada.',
            ed STRING COMMENT 'Endereço de localização da parada.',
            px DOUBLE COMMENT 'Latitude da localização da parada.',
            py DOUBLE COMMENT 'Longitude da localização da parada.',
            cod_linha INT COMMENT 'Código identificador da linha de ônibus consultada (FK).',
            dt_ingestao TIMESTAMP COMMENT 'Timestamp da ingestão dos dados na camada Bronze.',
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
    descricao = "Tabela Bronze: Relação de Paradas atendidas por cada Linha."
    spark.sql(f"COMMENT ON TABLE {tabela_full_name} IS '{descricao}'")
    
    display(df.limit(5))

else:
    print("❌ Nenhum dado encontrado.")
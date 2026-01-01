# Databricks notebook source
# MAGIC %md
# MAGIC Retorna a lista detalhada de todas as paradas que compõem um determinado corredor.

# COMMAND ----------

# ============================================================
# Ingestão incremental da API Olho Vivo - Paradas por Corredor
# Frequência: Diária
# Camada: Bronze (Azure Standard / Hive Metastore)
# ============================================================

import requests
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from datetime import datetime

# --- 1. CONFIGURAÇÕES ---
storage_account = "datalakeprojmba" 
container = "datalakeprojmba" 
meu_token = "6af34791de77cb4fe108edab6287fa51829405c1781e99e0e2d81cd0114cf3e7"

# Caminhos
base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
caminho_landing = f"{base_path}/olhovivo/bronze/landing_zone/paradas_por_corredor"
tabela_full_name = "olhovivo_bronze.paradas_por_corredor"
base_url = "https://api.olhovivo.sptrans.com.br/v2.1"

# --- LIMPEZA DE AMBIENTE (DEV) ---
caminho_tabela_fisico = f"{base_path}/olhovivo/bronze/paradas_por_corredor"
# Descomente para resetar
dbutils.fs.rm(caminho_tabela_fisico, True)
spark.sql(f"DROP TABLE IF EXISTS {tabela_full_name}")
print(f"🧹 Faxina completa em: {caminho_tabela_fisico}")


# --- 2. DEFINIÇÃO DO SCHEMA ---
schema_paradas = StructType([
    StructField("cp", IntegerType(), True), 
    StructField("np", StringType(), True),  
    StructField("ed", StringType(), True),  
    StructField("py", DoubleType(), True),  
    StructField("px", DoubleType(), True),  
    # Note: O campo cod_corredor será adicionado manualmente depois
])

# --- 3. AUTENTICAÇÃO ---
session = requests.Session()
auth = session.post(f"{base_url}/Login/Autenticar?token={meu_token}")

if auth.text.lower() != "true":
    raise Exception(f"❌ Falha na autenticação: {auth.text}")
print("✅ Autenticado.")

# --- 4. PREPARAÇÃO: BUSCAR LISTA DE CORREDORES ---
# Precisamos saber quais corredores existem para consultar suas paradas
print("🔍 Consultando lista de corredores ativos...")
resp_corredores = session.get(f"{base_url}/Corredor")
codigos_corredores = []

if resp_corredores.status_code == 200:
    lista_corredores = resp_corredores.json()
    # A API retorna: [{'cc': 1, 'nc': 'Amaro'}, {'cc': 2, ...}]
    codigos_corredores = [c['cc'] for c in lista_corredores]
    print(f"🔹 Encontrados {len(codigos_corredores)} corredores para processar.")
else:
    raise Exception(f"Erro ao buscar corredores: {resp_corredores.status_code}")

# --- 5. COLETA DE DADOS (LOOP) ---
lista_dfs = []
total_consultas = 0
consultas_sucesso = 0

for codigo in codigos_corredores:
    total_consultas += 1
    # url = f"{base_url}/Parada/BuscarParadasPorCorredor?codigoCorredor={codigo}"
    
    try:
        resp = session.get(
            f"{base_url}/Parada/BuscarParadasPorCorredor",
            params={"codigoCorredor": codigo},
            timeout=10
        )

        if resp.status_code == 200:
            dados = resp.json()
            
            if len(dados) > 0:
                consultas_sucesso += 1
                # Criamos o DF parcial
                df_temp = spark.createDataFrame(dados, schema=schema_paradas)
                
                # ADIÇÃO IMPORTANTE: Identificar de qual corredor veio essa parada
                df_temp = df_temp.withColumn("cod_corredor", lit(codigo))
                
                lista_dfs.append(df_temp)
                print(f"   ✅ Corredor {codigo}: {len(dados)} paradas.")
            else:
                print(f"   ⚠️ Corredor {codigo}: 0 paradas.")
        else:
            print(f"   ⚠️ Erro HTTP {resp.status_code} no corredor {codigo}")

    except Exception as e:
        print(f"   ❌ Erro técnico corredor {codigo}: {e}")
    
    # Pausa leve para não tomar block da API (Good Practice)
    time.sleep(0.2)

print("="*40)
print(f"Resumo: {consultas_sucesso} corredores com dados de {len(codigos_corredores)}.")
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
           .withColumn("arquivo_origem", lit(f"paradas_corredor_{timestamp_str}.json"))
    
    total_registros = df.count()
    print(f"\n✅ Total paradas carregadas: {total_registros}")

    # A. Salva JSON Raw
    caminho_arquivo_json = f"{caminho_landing}/paradas_corredor_{timestamp_str}.json"
    df.write.mode("overwrite").json(caminho_arquivo_json)
    print(f"🗂️ JSON salvo em: {caminho_arquivo_json}")

    # B. Salva Delta (Bronze)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tabela_full_name} (
            cp INT COMMENT 'Código identificador da parada.',
            np STRING COMMENT 'Nome da parada.',
            ed STRING COMMENT 'Endereço da localização da parada.',
            px DOUBLE COMMENT 'Longitude.',
            py DOUBLE COMMENT 'Latitude.',
            cod_corredor INT COMMENT 'Código do Corredor (Origem).',
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
    descricao = "Tabela Bronze: Paradas associadas a Corredores."
    spark.sql(f"COMMENT ON TABLE {tabela_full_name} IS '{descricao}'")
    
    display(df.limit(5))

else:
    print("❌ Nenhum dado encontrado.")
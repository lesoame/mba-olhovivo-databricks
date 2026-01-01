# Databricks notebook source
# MAGIC %md
# MAGIC Retorna uma lista com a previsão de chegada de cada um dos veículos da linha informada em todos os pontos de parada aos quais que ela atende.

# COMMAND ----------

# ============================================================
# Ingestão incremental: Previsão de Chegada (Endpoint /Previsao/Linha)
# Frequência: Alta frequência (Real-time)
# Camada: Bronze (Azure Standard / Hive Metastore)
# ============================================================

import requests
import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, DoubleType
from datetime import datetime

# --- 1. CONFIGURAÇÕES ---
storage_account = "datalakeprojmba" 
container = "datalakeprojmba" 
meu_token = "6af34791de77cb4fe108edab6287fa51829405c1781e99e0e2d81cd0114cf3e7"

# Caminhos
base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
caminho_landing = f"{base_path}/olhovivo/bronze/landing_zone/previsao_linha"
tabela_full_name = "olhovivo_bronze.previsao_chegada_linha"
base_url = "https://api.olhovivo.sptrans.com.br/v2.1"

# Batch Size menor pois a resposta desse endpoint é GRANDE (traz todas as paradas da linha)
BATCH_SIZE = 20 

# --- LIMPEZA DE AMBIENTE (DEV) ---
# dbutils.fs.rm(f"{base_path}/olhovivo/bronze/previsao_chegada_linha", True)
#spark.sql(f"DROP TABLE IF EXISTS {tabela_full_name}")


# --- 2. DEFINIÇÃO DO SCHEMA (FLATTENED) ---
schema_previsao_linha = StructType([
    StructField("hr", StringType(), True),      
    StructField("cod_linha", IntegerType(), True), 
    StructField("cp", IntegerType(), True),     
    StructField("np", StringType(), True),      
    StructField("py_parada", DoubleType(), True), 
    StructField("px_parada", DoubleType(), True), 
    StructField("p", StringType(), True),       
    StructField("t", StringType(), True),       
    StructField("a", BooleanType(), True),      
    StructField("ta", StringType(), True),      
    StructField("py", DoubleType(), True),      
    StructField("px", DoubleType(), True)       
])

# --- 3. AUTENTICAÇÃO ---
session = requests.Session()
auth = session.post(f"{base_url}/Login/Autenticar?token={meu_token}")
if auth.text.lower() != "true":
    raise Exception(f"❌ Falha na autenticação: {auth.text}")
print("✅ Autenticado.")


# --- 4. PREPARAÇÃO: OBTER LISTA DE LINHAS ---
print("🔍 Lendo lista de linhas ativas (olhovivo_bronze.buscar_linhas)...")
try:
    # Usamos a tabela que já ingerimos para saber quais linhas consultar
    df_linhas = spark.table("olhovivo_bronze.buscar_linhas")
    
    # ⚠️ LIMITADOR DE SEGURANÇA (MBA) ⚠️
    # Selecionamos 50 linhas aleatórias para teste.
    # Remova o .limit(50) para produção, mas vai demorar bastante.
    lista_codigos = [row['cl'] for row in df_linhas.select('cl').distinct().limit(50).collect()]
    
    print(f"🔹 Linhas selecionadas para consulta: {len(lista_codigos)}")

except Exception as e:
    raise Exception(f"Erro ao ler tabela de linhas: {e}")


# --- 5. LOOP DE PROCESSAMENTO ---
total_registros_geral = 0
timestamp_geral = datetime.now().strftime("%Y%m%d_%H%M%S")

def achatar_json_linha(dados_json, codigo_linha):
    flat_list = []
    if not dados_json: return flat_list
    
    hr = dados_json.get("hr")
    # A API retorna um objeto "ps" (paradas) que contém uma lista
    ps_root = dados_json.get("ps") 
    
    if not ps_root: return flat_list
    paradas = [ps_root] if isinstance(ps_root, dict) else ps_root

    for p in paradas:
        # Dados da Parada
        cp = p.get("cp")
        np_name = p.get("np")
        py_par = p.get("py")
        px_par = p.get("px")
        
        # Lista de Veículos vindo para esta parada
        vs = p.get("vs", [])
        if not vs: continue

        for v in vs:
            flat_list.append({
                "hr": hr,
                "cod_linha": codigo_linha, # Importante para rastreio
                "cp": cp,
                "np": np_name,
                "py_parada": py_par,
                "px_parada": px_par,
                "p": str(v.get("p")), 
                "t": v.get("t"),
                "a": v.get("a"),
                "ta": v.get("ta"),
                "py": v.get("py"),
                "px": v.get("px")
            })
    return flat_list

# Loop Principal
for i in range(0, len(lista_codigos), BATCH_SIZE):
    lote_cods = lista_codigos[i:i+BATCH_SIZE]
    print(f"\n🚀 Processando lote {i//BATCH_SIZE + 1} ({len(lote_cods)} linhas)...")
    
    registros_lote = []
    
    for cl in lote_cods:
        try:
            resp = session.get(f"{base_url}/Previsao/Linha?codigoLinha={cl}", timeout=10)
            if resp.status_code == 200:
                registros_lote.extend(achatar_json_linha(resp.json(), cl))
            else:
                # Erro 404 é comum se a linha não estiver operando agora
                pass 
        except Exception as e:
            print(f"   Erro na linha {cl}: {e}")
            continue
    
    # Gravação
    if registros_lote:
        df_lote = spark.createDataFrame(registros_lote, schema=schema_previsao_linha)
        
        # Enriquecimento
        df_lote = df_lote.withColumn("dt_ingestao", current_timestamp()) \
                         .withColumn("arquivo_origem", lit(f"batch_{i}_{timestamp_geral}.json"))
        
        qtd = df_lote.count()
        total_registros_geral += qtd

        # Salva Raw e Delta
        caminho_raw = f"{caminho_landing}/batch_{i}_{timestamp_geral}.json"
        df_lote.write.mode("overwrite").json(caminho_raw)

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {tabela_full_name} (
                hr STRING COMMENT 'Horário de referência da geração das informações.', 
                cod_linha INT COMMENT 'Código identificador da linha de ônibus consultada (FK).', 
                cp INT COMMENT 'Código identificador da parada.', 
                np STRING COMMENT 'Nome da parada.', 
                py_parada DOUBLE COMMENT 'Informação de latitude da localização do veículo.', 
                px_parada DOUBLE COMMENT 'Informação de longitude da localização do veículo.', 
                p STRING COMMENT 'Prefixo do veículo.', 
                t STRING COMMENT 'Horário previsto para chegada do veículo no ponto de parada relacionado.', 
                a BOOLEAN COMMENT 'Indica se o veículo é (true) ou não (false) acessível para pessoas com deficiência.', 
                ta STRING COMMENT 'Indica o horário universal (UTC) em que a localização foi capturada. Essa informação está no padrão ISO 8601.', 
                py DOUBLE COMMENT 'Informação de latitude da localização do veículo.', 
                px DOUBLE COMMENT 'Informação de longitude da localização do veículo.',
                dt_ingestao TIMESTAMP COMMENT 'Timestamp da ingestão dos dados no ambiente Bronze.', 
                arquivo_origem STRING COMMENT 'Nome do arquivo JSON que originou os dados.'
            ) USING DELTA
        """)
        
        (df_lote.write
           .format("delta")
           .mode("append")
           .option("mergeSchema", "true") 
           .saveAsTable(tabela_full_name)
        )
        print(f"   ✅ {qtd} previsões salvas.")
    else:
        print("   ⚠️ Nenhuma previsão neste lote.")

print(f"\n🏁 Processamento finalizado. Total: {total_registros_geral}")
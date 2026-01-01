# Databricks notebook source
# ============================================================
# Ingestão incremental: Previsão de Chegada (Baseada no GTFS)
# Frequência: Alta frequência (Real-time)
# Camada: Bronze (Azure Standard / Hive Metastore)
# ============================================================

import requests
import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, DoubleType
from datetime import datetime

# --- 1. CONFIGURAÇÕES ---
storage_account = "datalakeprojmba" 
container = "datalakeprojmba" 
meu_token = "6af34791de77cb4fe108edab6287fa51829405c1781e99e0e2d81cd0114cf3e7"

# Caminhos
base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
caminho_landing = f"{base_path}/olhovivo/bronze/landing_zone/previsao_parada"
tabela_full_name = "olhovivo_bronze.previsao_chegada_parada"
base_url = "https://api.olhovivo.sptrans.com.br/v2.1"

# Tamanho do lote para salvar no Delta (evita perder tudo se der erro)
BATCH_SIZE = 500 

# --- LIMPEZA DE AMBIENTE (DEV) ---
# Se quiser zerar a tabela para começar limpo com o GTFS:
# dbutils.fs.rm(f"{base_path}/olhovivo/bronze/previsao_chegada_parada", True)
# spark.sql(f"DROP TABLE IF EXISTS {tabela_full_name}")


# --- 2. DEFINIÇÃO DO SCHEMA ---
schema_previsao = StructType([
    StructField("hr", StringType(), True),
    StructField("cp", IntegerType(), True),
    StructField("np", StringType(), True),
    StructField("py_parada", DoubleType(), True),
    StructField("px_parada", DoubleType(), True),
    StructField("c", StringType(), True),
    StructField("cl", IntegerType(), True),
    StructField("sl", IntegerType(), True),
    StructField("lt0", StringType(), True),
    StructField("lt1", StringType(), True),
    StructField("qv", IntegerType(), True),
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


# --- 4. PREPARAÇÃO: LER PARADAS DO GTFS (MUDANÇA AQUI) ---
print("🔍 Lendo cadastro mestre de paradas (GTFS)...")

try:
    # Lemos a tabela GTFS que criamos
    df_gtfs = spark.table("olhovivo_bronze.gtfs_stops")
    
    # Selecionamos os IDs (stop_id). 
    # O GTFS trata como String, a API precisa de Int.
    # Filtramos apenas os numéricos para evitar erros.
    df_ids = df_gtfs.select("stop_id").distinct()
    
    # ⚠️ LIMITADOR PARA O MBA ⚠️
    # O GTFS tem ~20.000 paradas. Consultar todas leva ~1 hora num loop simples.
    # Para o projeto, vamos pegar uma amostra de 100 paradas para provar o conceito.
    # Se quiser todas, remova o .limit(100).
    lista_rows = df_ids.collect()
    
    lista_ids = []
    for row in lista_rows:
        try:
            # Converte para int, pois a API espera número
            lista_ids.append(int(row['stop_id']))
        except:
            continue
            
    print(f"🔹 Paradas do GTFS carregadas para consulta: {len(lista_ids)}")

except Exception as e:
    raise Exception(f"Erro ao ler olhovivo_bronze.gtfs_stops. Verifique se rodou a ingestão GTFS! {e}")


# --- 5. LOOP DE PROCESSAMENTO (BATCH) ---
total_registros_geral = 0
timestamp_geral = datetime.now().strftime("%Y%m%d_%H%M%S")

# Função Helper
def achatar_json(dados_json):
    flat_list = []
    if not dados_json: return flat_list
    hr = dados_json.get("hr")
    p_root = dados_json.get("p")
    if not p_root: return flat_list
    paradas = [p_root] if isinstance(p_root, dict) else p_root

    for p in paradas:
        cp = p.get("cp")
        np_name = p.get("np")
        py_par = p.get("py")
        px_par = p.get("px")
        linhas = p.get("l", [])
        if not linhas: continue

        for l in linhas:
            vs = l.get("vs", [])
            for v in vs:
                flat_list.append({
                    "hr": hr, "cp": cp, "np": np_name, "py_parada": py_par, "px_parada": px_par,
                    "c": l.get("c"), "cl": l.get("cl"), "sl": l.get("sl"),
                    "lt0": l.get("lt0"), "lt1": l.get("lt1"), "qv": l.get("qv"),
                    "p": str(v.get("p")), "t": v.get("t"), "a": v.get("a"),
                    "ta": v.get("ta"), "py": v.get("py"), "px": v.get("px")
                })
    return flat_list

# Execução do Loop
for i in range(0, len(lista_ids), BATCH_SIZE):
    lote_ids = lista_ids[i:i+BATCH_SIZE]
    print(f"\n🚀 Processando lote {i//BATCH_SIZE + 1} ({len(lote_ids)} paradas)...")
    
    registros_lote = []
    
    for pid in lote_ids:
        try:
            resp = session.get(f"{base_url}/Previsao/Parada?codigoParada={pid}", timeout=5)
            if resp.status_code == 200:
                registros_lote.extend(achatar_json(resp.json()))
        except:
            continue # Se falhar uma parada, segue o baile
    
    # Grava se tiver dados
    if registros_lote:
        df_lote = spark.createDataFrame(registros_lote, schema=schema_previsao)
        
        # Enriquecimento
        df_lote = df_lote.withColumn("dt_ingestao", current_timestamp()) \
                         .withColumn("arquivo_origem", lit(f"gtfs_batch_{i}_{timestamp_geral}.json"))
        
        qtd = df_lote.count()
        total_registros_geral += qtd

        # Salva JSON Raw (Landing)
        caminho_raw = f"{caminho_landing}/gtfs_batch_{i}_{timestamp_geral}.json"
        df_lote.write.mode("overwrite").json(caminho_raw)

        # Salva Delta (Bronze)
        # Recria tabela se não existir
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {tabela_full_name} (
                hr STRING COMMENT 'Horário de referência da geração das informações.', 
                cp INT COMMENT 'Código identificador da parada.', 
                np STRING COMMENT 'Nome da parada.', 
                py_parada DOUBLE COMMENT 'Informação de latitude da localização do veículo.', 
                px_parada DOUBLE COMMENT 'Informação de longitude da localização do veículo.',
                c STRING COMMENT 'Letreiro completo.', 
                cl INT COMMENT 'Código identificador da linha.', 
                sl INT COMMENT 'Sentido de operação onde 1 significa de Terminal Principal para Terminal Secundário e 2 de Terminal Secundário para Terminal Principal.', 
                lt0 STRING COMMENT 'Letreiro de destino da linha.', 
                lt1 STRING COMMENT 'Letreiro de origem da linha.', 
                qv INT COMMENT 'Quantidade de veículos localizados.',
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
        print("   ⚠️ Nenhuma previsão (ônibus chegando) encontrada neste lote.")

print(f"\n🏁 Processamento GTFS finalizado. Total: {total_registros_geral}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM olhovivo_bronze.previsao_chegada_parada;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM olhovivo_bronze.previsao_chegada_parada LIMIT 10;

# COMMAND ----------

# ============================================================
# Atualização de Metadados (Comentários)
# Tabela: olhovivo_bronze.previsao_chegada_parada
# ============================================================

tabela_alvo = "olhovivo_bronze.previsao_chegada_parada"

# Dicionário com Coluna -> Descrição
comentarios_colunas = {
    "hr": "Horário de referência da geração das informações.",
    "cp": "Código identificador da parada.",
    "np": "Nome da parada.",
    "py_parada": "Latitude da localização da parada.", 
    "px_parada": "Longitude da localização da parada.",
    "c": "Letreiro completo.",
    "cl": "Código identificador da linha.",
    "sl": "Sentido de operação onde 1 significa de Terminal Principal para Terminal Secundário e 2 de Terminal Secundário para Terminal Principal.",
    "lt0": "Letreiro de destino da linha.",
    "lt1": "Letreiro de origem da linha.",
    "qv": "Quantidade de veículos localizados.",
    "p": "Prefixo do veículo.",
    "t": "Horário previsto para chegada do veículo no ponto de parada relacionado.",
    "a": "Indica se o veículo é (true) ou não (false) acessível para pessoas com deficiência.",
    "ta": "Indica o horário universal (UTC) em que a localização foi capturada. Essa informação está no padrão ISO 8601.",
    "py": "Informação de latitude da localização do veículo.",
    "px": "Informação de longitude da localização do veículo.",
    "dt_ingestao": "Timestamp da ingestão dos dados no ambiente Bronze.",
    "arquivo_origem": "Nome do arquivo JSON que originou os dados."
}

print(f"🔄 Iniciando atualização de comentários na tabela: {tabela_alvo}...")

# 1. Atualiza descrição da tabela
spark.sql(f"COMMENT ON TABLE {tabela_alvo} IS 'Tabela Bronze: Previsão de chegada dos veículos nas paradas (Origem: API Olho Vivo).'")

# 2. Atualiza colunas uma por uma
for coluna, comentario in comentarios_colunas.items():
    try:
        # Comando SQL para alterar apenas o comentário da coluna
        spark.sql(f"ALTER TABLE {tabela_alvo} ALTER COLUMN {coluna} COMMENT '{comentario}'")
        print(f"   ✅ Coluna '{coluna}' documentada.")
    except Exception as e:
        print(f"   ⚠️ Erro na coluna '{coluna}': {e}")

print("\n🏁 Atualização de metadados concluída!")

# 3. Validação: Mostra como ficou
display(spark.sql(f"DESCRIBE TABLE EXTENDED {tabela_alvo}"))
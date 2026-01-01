# Databricks notebook source
# MAGIC %md
# MAGIC A categoria Empresas possibilita uma consulta que retorna a relação das empresas operadoras do transporte público na cidade de São Paulo.

# COMMAND ----------

# ============================================================
# Ingestão incremental da API Olho Vivo - Empresas Operadoras
# Frequência sugerida: Diária ou Semanal (Cadastro estático)
# Camada: Bronze (Azure Standard / Hive Metastore)
# ============================================================

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType
from datetime import datetime

# --- 1. CONFIGURAÇÕES ---
storage_account = "datalakeprojmba" 
container = "datalakeprojmba" 
meu_token = "6af34791de77cb4fe108edab6287fa51829405c1781e99e0e2d81cd0114cf3e7"

# Caminhos
base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
caminho_landing = f"{base_path}/olhovivo/bronze/landing_zone/empresas"
tabela_full_name = "olhovivo_bronze.empresas"
base_url = "https://api.olhovivo.sptrans.com.br/v2.1"

# --- LIMPEZA DE AMBIENTE (DEV) ---
caminho_tabela_fisico = f"{base_path}/olhovivo/bronze/empresas"
# Descomente para resetar tabela
dbutils.fs.rm(caminho_tabela_fisico, True)
spark.sql(f"DROP TABLE IF EXISTS {tabela_full_name}")
print(f"🧹 Faxina completa em: {caminho_tabela_fisico}")


# --- 2. DEFINIÇÃO EXPLÍCITA DO SCHEMA ---
# Define como os dados devem entrar na tabela final (achatada)
schema_empresas = StructType([
    StructField("hr", StringType(), True),  
    StructField("a", IntegerType(), True),  
    StructField("c", IntegerType(), True),  
    StructField("n", StringType(), True)    
])

# --- 3. AUTENTICAÇÃO ---
session = requests.Session()
auth = session.post(f"{base_url}/Login/Autenticar?token={meu_token}")

if auth.text.lower() != "true":
    raise Exception(f"❌ Falha na autenticação: {auth.text}")
print("✅ Autenticado.")

# --- 4. COLETA E PARSEAMENTO ---
print("🔍 Consultando cadastro de empresas...")
try:
    resp = session.get(f"{base_url}/Empresa", timeout=10)
    
    if resp.status_code == 200:
        raw_data = resp.json()
        print(f"🔹 Tipo de retorno: {type(raw_data)}")
        
        # O JSON vem aninhado: { "hr": "...", "e": [ { "a": 1, "e": [...] } ] }
        # Vamos "achatar" isso numa lista de dicionários simples
        
        registros_achatados = []
        
        # Garante que seja lista para iterar
        if isinstance(raw_data, dict):
            # Se vier um objeto único, encapsula em lista, senão usa a lista direta
            items_root = [raw_data] 
        else:
            items_root = raw_data

        # Loop de Parseamento (Flattening)
        for item in items_root:
            hora_ref = item.get("hr")
            areas = item.get("e", []) # Lista de áreas
            
            for area in areas:
                cod_area = area.get("a")
                empresas = area.get("e", []) # Lista de empresas dentro da área
                
                for emp in empresas:
                    registros_achatados.append({
                        "hr": hora_ref,
                        "a": cod_area,
                        "c": emp.get("c"),
                        "n": emp.get("n")
                    })
        
        print(f"✅ Parseamento concluído. {len(registros_achatados)} empresas encontradas.")
        
        # Cria DataFrame aplicando o Schema Forte
        if registros_achatados:
            df = spark.createDataFrame(registros_achatados, schema=schema_empresas)
        else:
            df = None
            
    else:
        raise Exception(f"Erro na API: HTTP {resp.status_code}")

except Exception as e:
    raise Exception(f"❌ Erro na coleta: {e}")

# --- 5. PROCESSAMENTO E GRAVAÇÃO ---
if df:
    # Enriquecimento
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    df = df.withColumn("dt_ingestao", current_timestamp()) \
           .withColumn("arquivo_origem", lit(f"empresas_{timestamp_str}.json"))

    # A. Salva JSON Raw (Landing Zone)
    caminho_arquivo_json = f"{caminho_landing}/empresas_{timestamp_str}.json"
    df.write.mode("overwrite").json(caminho_arquivo_json)
    print(f"🗂️ JSON salvo em: {caminho_arquivo_json}")

    # B. Salva Delta (Bronze)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tabela_full_name} (
            hr STRING COMMENT 'Horário de referência da geração das informações.',
            a INT COMMENT 'Código da área de operação.',
            c INT COMMENT 'Código identificador da empresa.',
            n STRING COMMENT 'Nome da empresa.',
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
    descricao = "Tabela Bronze: Relação das empresas operadoras do transporte público de SP."
    spark.sql(f"COMMENT ON TABLE {tabela_full_name} IS '{descricao}'")
    
    display(df)

else:
    print("❌ Nenhum dado encontrado ou lista vazia.")
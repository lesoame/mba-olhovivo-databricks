# Databricks notebook source
# MAGIC %md
# MAGIC Realiza uma busca fonética das paradas de ônibus do sistema com base no parâmetro informado. A consulta é realizada no nome da parada e também no seu endereço de localização.

# COMMAND ----------

# ============================================================
# Ingestão incremental da API Olho Vivo - Paradas
# Frequência sugerida: Diária (Dados cadastrais mudam pouco)
# Camada: Bronze (Azure Standard / Hive Metastore)
# ============================================================

import requests
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
caminho_landing = f"{base_path}/olhovivo/bronze/landing_zone/paradas"
tabela_full_name = "olhovivo_bronze.buscar_paradas"
base_url = "https://api.olhovivo.sptrans.com.br/v2.1"

# --- LIMPEZA DE AMBIENTE (DEV) ---
# Executa a faxina para garantir que recriaremos a tabela sem conflitos
caminho_tabela_fisico = f"{base_path}/olhovivo/bronze/buscar_paradas"

# Descomente para resetar a tabela
dbutils.fs.rm(caminho_tabela_fisico, True)
spark.sql(f"DROP TABLE IF EXISTS {tabela_full_name}")
print(f"🧹 Faxina completa em: {caminho_tabela_fisico}")


# --- 2. DEFINIÇÃO EXPLÍCITA DO SCHEMA ---
# Fundamental para garantir coordenadas como Double e IDs como Int
schema_paradas = StructType([
    StructField("cp", IntegerType(), True), 
    StructField("np", StringType(), True),  
    StructField("ed", StringType(), True),  
    StructField("py", DoubleType(), True),  
    StructField("px", DoubleType(), True)   
])

# --- 3. AUTENTICAÇÃO ---
session = requests.Session()
auth = session.post(f"{base_url}/Login/Autenticar?token={meu_token}")

if auth.text.lower() == "true":
    print("✅ Autenticado com sucesso na API Olho Vivo")
else:
    raise Exception(f"❌ Falha na autenticação: {auth.text}")

# --- 4. COLETA DE DADOS (LOOP) ---
lista_dfs = []

# Nota: A busca por "1" a "9" traz paradas que contenham esses números no nome/endereço.
print("🔍 Buscando paradas com termos 1 a 9...")

for i in range(1, 10):
    try:
        resp = session.get(f"{base_url}/Parada/Buscar?termosBusca={i}", timeout=10)
        
        if resp.status_code == 200:
            dados = resp.json()
            print(f"   Termo '{i}': {len(dados)} registros retornados.")
            
            if len(dados) > 0:
                # APLICANDO O SCHEMA FORTE AQUI
                df_temp = spark.createDataFrame(dados, schema=schema_paradas)
                lista_dfs.append(df_temp)
        else:
            print(f"   ⚠️ Erro HTTP {resp.status_code} no termo {i}")

    except Exception as e:
        print(f"   ❌ Erro na consulta do termo {i}: {e}")
        continue

print("="*40)
print(f"DataFrames criados: {len(lista_dfs)}")
print("="*40)

# --- 5. PROCESSAMENTO E GRAVAÇÃO ---
if lista_dfs:
    # União
    df = lista_dfs[0]
    for df_temp in lista_dfs[1:]:
        df = df.union(df_temp)
    
    # IMPORTANTE: Remover duplicatas, pois o termo '1' e '2' podem trazer a mesma parada
    count_antes = df.count()
    df = df.dropDuplicates(['cp'])
    count_depois = df.count()

    print(f"\n✅ Total registros: {count_depois} (Removidas {count_antes - count_depois} duplicatas)")

    # Enriquecimento
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    df = df.withColumn("dt_ingestao", current_timestamp()) \
           .withColumn("arquivo_origem", lit(f"paradas_{timestamp_str}.json"))

    # A. Salva JSON Raw (Landing Zone)
    caminho_arquivo_json = f"{caminho_landing}/paradas_{timestamp_str}.json"
    df.write.mode("overwrite").json(caminho_arquivo_json)
    print(f"🗂️ JSON salvo em: {caminho_arquivo_json}")

    # B. Salva Delta (Bronze)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tabela_full_name} (
            cp INT COMMENT 'Código identificador da parada.',
            np STRING COMMENT 'Nome da parada.',
            ed STRING COMMENT 'Endereço de localização da parada.',
            px DOUBLE COMMENT 'Longitude.',
            py DOUBLE COMMENT 'Latitude.',
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

    # C. Metadados e Comentários
    descricao = "Tabela Bronze: Cadastro de Paradas (Pontos) da SPTrans."
    spark.sql(f"COMMENT ON TABLE {tabela_full_name} IS '{descricao}'")
    
    # Amostra
    display(df.limit(5))

else:
    print("❌ Nenhum dado encontrado.")
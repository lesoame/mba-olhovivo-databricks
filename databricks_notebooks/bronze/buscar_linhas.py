# Databricks notebook source
# MAGIC %md
# MAGIC A categoria Linhas possibilita a consulta pelas linhas de ônibus da cidade de São Paulo, bem como suas informações cadastrais como por exemplo: horário de operação da linha, dias de operação (dia útil, sábado ou domingo) e extensão da linha (em metros).
# MAGIC
# MAGIC Buscar
# MAGIC Realiza uma busca das linhas do sistema com base no parâmetro informado. Se a linha não é encontrada então é realizada uma busca fonetizada na denominação das linhas.

# COMMAND ----------

# DBTITLE 1,Criação da tabela buscar_linhas e gravação dos dados no Azure
# ============================================================
# Ingestão incremental da API Olho Vivo - Linhas
# Frequência: a cada 15 minutos
# Camada: Bronze (Azure Standard / Hive Metastore)
# ============================================================

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, LongType
from datetime import datetime

# --- 1. CONFIGURAÇÕES ---
storage_account = "datalakeprojmba" 
container = "datalakeprojmba"              
meu_token = "6af34791de77cb4fe108edab6287fa51829405c1781e99e0e2d81cd0114cf3e7"

# Caminhos
base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
caminho_landing = f"{base_path}/olhovivo/bronze/landing_zone/linhas"
tabela_full_name = "olhovivo_bronze.buscar_linhas"
base_url = "https://api.olhovivo.sptrans.com.br/v2.1"

# --- LIMPEZA DE AMBIENTE (DEV) ---
# Isso apaga os arquivos físicos antigos para permitir criar a tabela do zero novamente.
# Só rode isso se der o erro "Location is not empty".

caminho_tabela_fisico = f"{base_path}/olhovivo/bronze/buscar_linhas"

# O comando abaixo deleta a pasta e tudo dentro dela
dbutils.fs.rm(caminho_tabela_fisico, True)

# Também garantimos que o Databricks esqueça qualquer metadado antigo
spark.sql(f"DROP TABLE IF EXISTS {tabela_full_name}")

print(f"🧹 Faxina completa em: {caminho_tabela_fisico}")
print("Pode rodar o restante do código agora!")

# --- 2. DEFINIÇÃO EXPLÍCITA DO SCHEMA (A Solução do Erro) ---
# Isso força o Spark a converter os dados para os tipos corretos
schema_linhas = StructType([
    StructField("cl", IntegerType(), True), 
    StructField("lc", BooleanType(), True), 
    StructField("lt", StringType(), True),  
    StructField("sl", IntegerType(), True), 
    StructField("tl", IntegerType(), True), 
    StructField("tp", StringType(), True),  
    StructField("ts", StringType(), True)   
])

# --- 3. AUTENTICAÇÃO ---
session = requests.Session()
auth = session.post(f"{base_url}/Login/Autenticar?token={meu_token}")
if auth.text.lower() != "true":
    raise Exception(f"❌ Falha na autenticação: {auth.text}")
print("✅ Autenticado.")

# --- 4. COLETA DE DADOS ---
lista_dfs = []

print("🔍 Iniciando varredura...")
for i in range(1, 10):
    try:
        resp = session.get(f"{base_url}/Linha/Buscar?termosBusca={i}", timeout=10)
        if resp.status_code == 200:
            dados = resp.json()
            if len(dados) > 0:
                # O PULO DO GATO: Passamos o schema aqui!
                # O Spark vai tentar converter (cast) automaticamente o JSON para este schema
                df_temp = spark.createDataFrame(dados, schema=schema_linhas)
                lista_dfs.append(df_temp)
    except Exception as e:
        print(f"Erro no termo {i}: {e}")
        continue

# --- 5. GRAVAÇÃO ---
if lista_dfs:
    # Une todos os DataFrames (agora todos têm o mesmo schema garantido)
    df = lista_dfs[0]
    for df_temp in lista_dfs[1:]:
        df = df.union(df_temp)

    # Enriquecimento
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    df = df.withColumn("dt_ingestao", current_timestamp()) \
           .withColumn("arquivo_origem", lit(f"linhas_{timestamp_str}.json"))

    print(f"✅ Total registros: {df.count()}")

    # A. Salva JSON Raw (Landing)
    df.write.mode("overwrite").json(f"{caminho_landing}/linhas_{timestamp_str}.json")
    
    # B. Salva Delta (Bronze)
    # Importante: Como deu erro antes, vamos recriar a tabela do zero para garantir
    # Se quiser limpar a sujeira anterior, descomente a linha abaixo:
    # spark.sql(f"DROP TABLE IF EXISTS {tabela_full_name}")
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tabela_full_name} (
            cl INT COMMENT 'Código identificador da linha. Este é um código identificador único de cada linha do sistema (por sentido de operação).',
            lc BOOLEAN COMMENT 'Indica se uma linha opera no modo circular (sem um terminal secundário).',
            lt STRING COMMENT 'Informa a primeira parte do letreiro numérico da linha.',
            sl INT COMMENT 'Informa o sentido ao qual a linha atende, onde 1 significa Terminal Principal para Terminal Secundário e 2 do Terminal Secundário para Terminal Principal.',
            tl INT COMMENT 'Informa a segunda parte do letreiro numérico da linha, que indica se a linha opera nos modos: BASE (10), ATENDIMENTO (21, 23, 32, 41).',
            tp STRING COMMENT 'Informa o letreiro descritivo da linha no sentido Terminal Principal para Terminal Secundário.',
            ts STRING COMMENT 'Informa o letreiro descritivo da linha no sentido Terminal Secundário para Terminal Principal.',
            dt_ingestao TIMESTAMP COMMENT 'Timestamp da ingestão dos dados no ambiente Bronze.',
            arquivo_origem STRING COMMENT 'Nome do arquivo JSON que originou os dados.'
        ) USING DELTA
    """)

    # Agora a gravação deve fluir sem erro 22005
    (df.write
       .format("delta")
       .mode("append")
       .option("mergeSchema", "true") 
       .saveAsTable(tabela_full_name)
    )
    
    print(f"🚀 Sucesso! Dados gravados em {tabela_full_name}")
    display(df.limit(5))
else:
    print("Nenhum dado encontrado.")
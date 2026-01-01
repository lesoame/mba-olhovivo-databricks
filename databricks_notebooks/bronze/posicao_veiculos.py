# Databricks notebook source
# MAGIC %md
# MAGIC A categoria Posição Dos Veículos é a responsável por retornar a posição exata de cada veículo de qualquer linha de ônibus da SPTrans

# COMMAND ----------

# ============================================================
# Ingestão incremental da API Olho Vivo - Posição (FATO)
# Frequência: A cada 5, 10 ou 15 minutos
# Camada: Bronze (Azure Standard / Hive Metastore)
# Versão: Com Retry e Tratamento de Erros
# ============================================================

import requests
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, DoubleType
from datetime import datetime

# --- 0. FUNÇÕES AUXILIARES (RETRY LOGIC) ---
def buscar_dados_com_retry(session, url, tentativas=3):
    """
    Tenta buscar dados na API. Se falhar, espera e tenta de novo.
    Retorna o JSON dos dados ou None se falhar todas as vezes.
    """
    for i in range(tentativas):
        try:
            print(f"🔄 Tentativa {i+1} de {tentativas}...")
            response = session.get(url, timeout=30) # Timeout aumentado para 30s
            
            if response.status_code == 200:
                dados = response.json()
                if dados: # Verifica se não veio vazio
                    return dados
                else:
                    print("⚠️ API retornou 200 OK mas o conteúdo estava vazio.")
            else:
                print(f"⚠️ Erro HTTP: {response.status_code}")
                
        except Exception as e:
            print(f"⚠️ Erro de conexão na tentativa {i+1}: {e}")
        
        # Espera 5 segundos antes da próxima tentativa (Backoff)
        time.sleep(5)
    
    print("❌ Falha total após todas as tentativas.")
    return None

# --- 1. CONFIGURAÇÕES ---
storage_account = "datalakeprojmba" 
container = "datalakeprojmba" 
meu_token = "6af34791de77cb4fe108edab6287fa51829405c1781e99e0e2d81cd0114cf3e7"

# Caminhos
base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
caminho_landing = f"{base_path}/olhovivo/bronze/landing_zone/posicao"
tabela_full_name = "olhovivo_bronze.posicao_veiculos"
base_url = "https://api.olhovivo.sptrans.com.br/v2.1"

# --- 2. DEFINIÇÃO EXPLÍCITA DO SCHEMA (OUTPUT) ---
schema_final = StructType([
    StructField("hr", StringType(), True),  
    StructField("cl", IntegerType(), True), 
    StructField("c", StringType(), True),   
    StructField("sl", IntegerType(), True), 
    StructField("lt1", StringType(), True), 
    StructField("lt0", StringType(), True), 
    StructField("p", StringType(), True),   
    StructField("a", BooleanType(), True),  
    StructField("ta", StringType(), True),  
    StructField("py", DoubleType(), True),  
    StructField("px", DoubleType(), True)   
])

# --- 3. AUTENTICAÇÃO ---
session = requests.Session()
try:
    auth = session.post(f"{base_url}/Login/Autenticar?token={meu_token}")
    if auth.text.lower() != "true":
        raise Exception(f"❌ Falha na autenticação: {auth.text}")
    print("✅ Autenticado com sucesso.")
except Exception as e:
    print(f"❌ Erro fatal na autenticação: {e}")
    dbutils.notebook.exit("Falha Auth") # Encerra o notebook se não autenticar

# --- 4. COLETA E LANDING (COM RETRY) ---
print("🔍 Buscando posição global dos veículos...")

# >>> AQUI ESTÁ A MUDANÇA PRINCIPAL <<<
# Usamos a função criada lá em cima em vez de chamar direto
raw_data = buscar_dados_com_retry(session, f"{base_url}/Posicao")

df = None # Inicializa variável

if raw_data:
    hora_ref = raw_data.get("hr", "00:00")
    
    # --- A. GRAVAÇÃO NA LANDING ZONE (SEGURANÇA) ---
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    caminho_arquivo_json = f"{caminho_landing}/posicao_{timestamp_str}.json"
    
    try:
        dbutils.fs.put(caminho_arquivo_json, json.dumps(raw_data), overwrite=True)
        print(f"🗂️ JSON bruto salvo em: {caminho_arquivo_json}")
    except Exception as e:
        print(f"⚠️ Aviso: Não foi possível salvar na Landing Zone, mas seguindo fluxo: {e}")

    # --- B. TRANSFORMAÇÃO (FLATTENING) ---
    lista_veiculos = []
    
    # O .get() aqui é seguro porque já checamos 'if raw_data'
    linhas_api = raw_data.get("l", [])
    
    if linhas_api:
        print(f"🔹 Processando {len(linhas_api)} linhas operacionais...")

        for linha in linhas_api:
            # Proteção extra: verifica se 'linha' não é None
            if linha: 
                veiculos = linha.get("vs", [])
                for v in veiculos:
                    if v: # Proteção extra para veículo
                        lista_veiculos.append({
                            "hr": hora_ref,
                            "cl": linha.get("cl"),
                            "c": linha.get("c"),
                            "sl": linha.get("sl"),
                            "lt1": linha.get("lt1"),
                            "lt0": linha.get("lt0"),
                            "p": v.get("p"),
                            "a": v.get("a"),
                            "ta": v.get("ta"),
                            "py": v.get("py"),
                            "px": v.get("px")
                        })
        
        print(f"✅ Total de veículos mapeados: {len(lista_veiculos)}")

        if lista_veiculos:
            df = spark.createDataFrame(lista_veiculos, schema=schema_final)
        else:
            print("⚠️ Lista de veículos vazia após processamento.")
    else:
        print("⚠️ Objeto 'l' (linhas) veio vazio da API.")

else:
    # Se raw_data for None (falhou as 3 tentativas)
    print("❌ Abortando: Não foi possível obter dados da API após múltiplas tentativas.")
    # Opcional: Encerrar com erro para o Job saber que falhou
    # dbutils.notebook.exit("Falha na Coleta")

# --- 5. GRAVAÇÃO DELTA ---
if df:
    # Enriquecimento
    df = df.withColumn("dt_ingestao", current_timestamp()) \
           .withColumn("arquivo_origem", lit(f"posicao_{timestamp_str}.json"))

    # Criação da Tabela (DDL)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tabela_full_name} (
            hr STRING COMMENT 'Horário de referência do conjunto de dados retornado pela API.',
            cl INT COMMENT 'Código identificador da linha.',
            c STRING COMMENT 'Letreiro completo da linha (exemplo: "1012-10").',
            sl INT COMMENT 'Sentido da linha: 1 = Terminal Principal → Terminal Secundário; 2 = inverso.',
            lt1 STRING COMMENT 'Nome do Terminal Principal da linha.',
            lt0 STRING COMMENT 'Nome do Terminal Secundário da linha.',
            p STRING COMMENT 'Prefixo do veículo (identificador único do ônibus).',
            a BOOLEAN COMMENT 'Indica se o veículo é acessível (PNE).',
            ta STRING COMMENT 'Timestamp UTC da última atualização da posição do veículo.',
            py DOUBLE COMMENT 'Latitude da posição atual do veículo.',
            px DOUBLE COMMENT 'Longitude da posição atual do veículo.',
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
    
    # Resumo
    print("Distribuição por Sentido:")
    df.groupBy("sl").count().show()

else:
    print("⚠️ Pipeline finalizado sem gravação (dados vazios ou erro na coleta).")
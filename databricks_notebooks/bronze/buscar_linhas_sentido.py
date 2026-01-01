# Databricks notebook source
# MAGIC %md
# MAGIC Realiza uma busca das linhas do sistema com base no parâmetro informado. Se a linha não é encontrada então é realizada uma busca fonetizada na denominação das linhas. A linha retornada será unicamente aquela cujo sentido de operação seja o informado no parâmetro sentido

# COMMAND ----------

# ============================================================
# Ingestão incremental da API Olho Vivo - Linhas por Sentido
# Frequência: a cada 15 minutos
# Camada: Bronze (Azure Standard / Hive Metastore)
# ============================================================

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType
from datetime import datetime

# --- 1. CONFIGURAÇÕES ---
storage_account = "datalakeprojmba" 
container = "datalakeprojmba" 
meu_token = "6af34791de77cb4fe108edab6287fa51829405c1781e99e0e2d81cd0114cf3e7"

# Caminhos
base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
caminho_landing = f"{base_path}/olhovivo/bronze/landing_zone/linhas_sentido"
tabela_full_name = "olhovivo_bronze.buscar_linhas_sentido"
base_url = "https://api.olhovivo.sptrans.com.br/v2.1"

# --- LIMPEZA DE AMBIENTE (DEV) ---
# Executa a faxina para garantir que recriaremos a tabela sem conflitos de metadados
caminho_tabela_fisico = f"{base_path}/olhovivo/bronze/buscar_linhas_sentido"

# Descomente as linhas abaixo se precisar "zerar" a tabela
dbutils.fs.rm(caminho_tabela_fisico, True)
spark.sql(f"DROP TABLE IF EXISTS {tabela_full_name}")
print(f"🧹 Faxina completa em: {caminho_tabela_fisico}")


# --- 2. DEFINIÇÃO EXPLÍCITA DO SCHEMA ---
# Essencial para evitar erro de INT vs BIGINT
schema_linhas_sentido = StructType([
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

if auth.text.lower() == "true":
    print("✅ Autenticado com sucesso na API Olho Vivo")
else:
    raise Exception(f"❌ Falha na autenticação: {auth.text}")

# --- 4. COLETA DE DADOS (LOOP) ---
lista_dfs = []
total_consultas = 0
consultas_sucesso = 0

print("🔍 Iniciando varredura (Linhas 1-9 x Sentidos 1-2)...")

for codigo_linha in range(1, 10):
    for sentido in range(1, 3):
        total_consultas += 1
        # url = f"{base_url}/Linha/BuscarLinhaSentido?termosBusca={codigo_linha}&sentido={sentido}"
        
        try:
            # Usando params do requests fica mais limpo e seguro
            resp = session.get(
                f"{base_url}/Linha/BuscarLinhaSentido", 
                params={"termosBusca": codigo_linha, "sentido": sentido},
                timeout=10
            )
            
            if resp.status_code == 200:
                dados = resp.json()
                
                if len(dados) > 0:
                    consultas_sucesso += 1
                    # APLICANDO O SCHEMA AQUI PARA EVITAR ERRO DE TIPO
                    df_temp = spark.createDataFrame(dados, schema=schema_linhas_sentido)
                    lista_dfs.append(df_temp)
            else:
                print(f"   ⚠️ Erro HTTP {resp.status_code} no termo {codigo_linha}/{sentido}")
                
        except Exception as e:
            print(f"   ❌ Erro na consulta {codigo_linha}/{sentido}: {str(e)}")
            continue

print("="*30)
print(f"Resumo: {consultas_sucesso} sucessos em {total_consultas} tentativas.")
print("="*30)

# --- 5. PROCESSAMENTO E GRAVAÇÃO ---
if lista_dfs:
    # União
    df = lista_dfs[0]
    for df_temp in lista_dfs[1:]:
        df = df.union(df_temp)

    # Enriquecimento
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    df = df.withColumn("dt_ingestao", current_timestamp()) \
           .withColumn("arquivo_origem", lit(f"linhas_sentido_{timestamp_str}.json"))
           
    print(f"✅ Total de registros combinados: {df.count()}")

    # A. Salva JSON Raw (Landing Zone)
    caminho_arquivo_json = f"{caminho_landing}/linhas_sentido_{timestamp_str}.json"
    df.write.mode("overwrite").json(caminho_arquivo_json)
    print(f"🗂️ JSON salvo em: {caminho_arquivo_json}")

    # B. Salva Delta (Bronze)
    # Criação da tabela com tipagem correta
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tabela_full_name} (
            cl INT COMMENT 'Código identificador da linha. Único por linha e sentido.',
            lc BOOLEAN COMMENT 'Indica se a linha é circular (sem terminal secundário).',
            lt STRING COMMENT 'Primeira parte do letreiro numérico da linha.',
            sl INT COMMENT 'Sentido da linha: 1 = Terminal Principal → Terminal Secundário; 2 = inverso.',
            tl INT COMMENT 'Letreiro numérico (Parte 2).',
            tp STRING COMMENT 'Letreiro descritivo da linha no sentido Terminal Principal → Terminal Secundário.',
            ts STRING COMMENT 'Letreiro descritivo da linha no sentido Terminal Secundário → Terminal Principal.',
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

    # C. Metadados e Comentários (Documentation)
    descricao = "Tabela Bronze: Linhas filtradas por Sentido (1 ou 2)."
    spark.sql(f"COMMENT ON TABLE {tabela_full_name} IS '{descricao}'")
    
    # Estatísticas Rápidas
    print("\nDistribuição por Sentido:")
    df.groupBy("sl").count().show()
    
    display(df.limit(5))

else:
    print("❌ Nenhum dado encontrado. Verifique se a API está online.")
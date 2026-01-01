-- Databricks notebook source
-- DBTITLE 1,Criação dos Schemas
-- No plano Standard, usamos o underscore (_) para separar projeto e camada
CREATE SCHEMA IF NOT EXISTS olhovivo_bronze
LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/';

CREATE SCHEMA IF NOT EXISTS olhovivo_silver
LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/silver/';

CREATE SCHEMA IF NOT EXISTS olhovivo_gold
LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/gold/';

-- COMMAND ----------

-- DBTITLE 1,Criação das tabelas na camada bronze apontando para os dados salvos na azure
-- buscar_linhas
CREATE TABLE IF NOT EXISTS olhovivo_bronze.buscar_linhas 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/buscar_linhas';

-- buscar_linhas_sentido
CREATE TABLE IF NOT EXISTS olhovivo_bronze.buscar_linhas_sentido 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/buscar_linhas_sentido';

-- buscar_paradas
CREATE TABLE IF NOT EXISTS olhovivo_bronze.buscar_paradas 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/buscar_paradas';

-- buscar_paradas_por_corredor
CREATE TABLE IF NOT EXISTS olhovivo_bronze.paradas_por_corredor 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/paradas_por_corredor';

-- buscar_paradas_por_linha
CREATE TABLE IF NOT EXISTS olhovivo_bronze.paradas_por_linha 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/paradas_por_linha';

-- corredores
CREATE TABLE IF NOT EXISTS olhovivo_bronze.corredores 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/corredores';

-- empresas
CREATE TABLE IF NOT EXISTS olhovivo_bronze.empresas 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/empresas';

-- posicao_veiculos
CREATE TABLE IF NOT EXISTS olhovivo_bronze.posicao_veiculos 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/posicao_veiculos';

-- previsao_chegada_parada
CREATE TABLE IF NOT EXISTS olhovivo_bronze.previsao_chegada_parada 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/previsao_chegada_parada';

-- previsao_chegada_parada_linha
CREATE TABLE IF NOT EXISTS olhovivo_bronze.previsao_chegada_linha 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/previsao_chegada_linha';

-- gtfs_routes
CREATE TABLE IF NOT EXISTS olhovivo_bronze.gtfs_routes 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/gtfs_routes';

-- gtfs_stops
CREATE TABLE IF NOT EXISTS olhovivo_bronze.gtfs_stops 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/gtfs_stops';

-- gtfs_agency
CREATE TABLE IF NOT EXISTS olhovivo_bronze.gtfs_agency 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/gtfs_agency';

-- gtfs_trips
CREATE TABLE IF NOT EXISTS olhovivo_bronze.gtfs_trips 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/gtfs_trips';

-- gtfs_shapes
CREATE TABLE IF NOT EXISTS olhovivo_bronze.gtfs_shapes 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/gtfs_shapes';

-- gtfs_fare_attributes
CREATE TABLE IF NOT EXISTS olhovivo_bronze.gtfs_fare_attributes 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/gtfs_fare_attributes';

-- gtfs_fare_rules
CREATE TABLE IF NOT EXISTS olhovivo_bronze.gtfs_fare_rules 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/gtfs_fare_rules';

-- gtfs_calendar
CREATE TABLE IF NOT EXISTS olhovivo_bronze.gtfs_calendar 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/gtfs_calendar';

-- gtfs_frequencies
CREATE TABLE IF NOT EXISTS olhovivo_bronze.gtfs_frequencies 
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/gtfs_frequencies';

-- gtfs_stop_times
CREATE TABLE IF NOT EXISTS olhovivo_bronze.gtfs_stop_times
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/gtfs_stop_times';

-- COMMAND ----------

-- DBTITLE 1,Criação das tabelas na camada silver apontando para os dados salvos na azure
-- dim_empresas
CREATE TABLE IF NOT EXISTS olhovivo_silver.dim_empresas
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/silver/dim_empresas';

-- dim_linhas
CREATE TABLE IF NOT EXISTS olhovivo_silver.dim_linhas
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/silver/dim_linhas';

-- fato_posicao_veiculos
CREATE TABLE IF NOT EXISTS olhovivo_silver.fato_posicao_veiculos
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/silver/fato_posicao_veiculos';

-- mapa_shapes_gtfs
CREATE TABLE IF NOT EXISTS olhovivo_silver.mapa_shapes_gtfs
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/silver/mapa_shapes_gtfs';

-- COMMAND ----------

-- DBTITLE 1,Criação das tabelas na camada gold apontando para os dados salvos na azure
-- acessibilidade
CREATE TABLE IF NOT EXISTS olhovivo_gold.acessibilidade
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/gold/acessibilidade';

-- velocidade_linhas
CREATE TABLE IF NOT EXISTS olhovivo_gold.velocidade_linhas
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/gold/velocidade_linhas';

-- snapshot_frota_atual
CREATE TABLE IF NOT EXISTS olhovivo_gold.snapshot_frota_atual
USING DELTA LOCATION 'abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/gold/snapshot_frota_atual';

-- COMMAND ----------

-- DBTITLE 1,Criação das tabelas GTFS na camada bronze
-- MAGIC %python
-- MAGIC # ============================================================
-- MAGIC # Ingestão de Dados Estáticos (GTFS)
-- MAGIC # Camada: Bronze (Azure Standard)
-- MAGIC # ============================================================
-- MAGIC
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from pyspark.sql.functions import current_timestamp, lit, input_file_name
-- MAGIC from datetime import datetime
-- MAGIC
-- MAGIC spark = SparkSession.builder.getOrCreate()
-- MAGIC
-- MAGIC # --- 1. CONFIGURAÇÕES ---
-- MAGIC storage_account = "datalakeprojmba"
-- MAGIC container = "datalakeprojmba"
-- MAGIC base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
-- MAGIC
-- MAGIC # Pasta onde você fez o upload no Azure
-- MAGIC caminho_origem = f"{base_path}/olhovivo/bronze/gtfs_upload"
-- MAGIC
-- MAGIC # Lista dos arquivos padrão do GTFS
-- MAGIC # O script vai procurar por esses nomes exatos na pasta
-- MAGIC arquivos_gtfs = [
-- MAGIC     "agency",
-- MAGIC     "calendar",
-- MAGIC     "fare_attributes",
-- MAGIC     "fare_rules",
-- MAGIC     "frequencies", 
-- MAGIC     "routes",
-- MAGIC     "shapes",
-- MAGIC     "stop_times",
-- MAGIC     "stops",
-- MAGIC     "trips"
-- MAGIC ]
-- MAGIC
-- MAGIC timestamp_ingestao = datetime.now()
-- MAGIC
-- MAGIC # --- 2. PROCESSAMENTO ---
-- MAGIC print(f"📂 Buscando arquivos em: {caminho_origem}")
-- MAGIC
-- MAGIC for arquivo in arquivos_gtfs:
-- MAGIC     print(f"\n🔄 Processando {arquivo}...")
-- MAGIC     
-- MAGIC     # Monta o caminho: .../gtfs_upload/stops.txt
-- MAGIC     path_file = f"{caminho_origem}/{arquivo}.txt"
-- MAGIC     
-- MAGIC     try:
-- MAGIC         # Lê o TXT como CSV (o GTFS é separado por vírgula)
-- MAGIC         df = (spark.read
-- MAGIC               .format("csv")
-- MAGIC               .option("header", "true")
-- MAGIC               .option("inferSchema", "true") # Pode inferir tipos para agilizar na Bronze
-- MAGIC               .option("delimiter", ",")
-- MAGIC               .load(path_file))
-- MAGIC         
-- MAGIC         # Adiciona metadados
-- MAGIC         df = df.withColumn("dt_ingestao", lit(timestamp_ingestao)) \
-- MAGIC                .withColumn("arquivo_origem", input_file_name())
-- MAGIC
-- MAGIC         # Define nome da tabela: olhovivo_bronze.gtfs_stops
-- MAGIC         nome_tabela = f"olhovivo_bronze.gtfs_{arquivo}"
-- MAGIC         
-- MAGIC         # Grava como tabela Delta (Overwrite, pois GTFS é cadastro mestre)
-- MAGIC         df.write \
-- MAGIC           .format("delta") \
-- MAGIC           .mode("overwrite") \
-- MAGIC           .option("overwriteSchema", "true") \
-- MAGIC           .saveAsTable(nome_tabela)
-- MAGIC           
-- MAGIC         print(f"✅ Tabela criada com sucesso: {nome_tabela} ({df.count()} linhas)")
-- MAGIC         
-- MAGIC     except Exception as e:
-- MAGIC         print(f"⚠️ Erro ao ler {arquivo}.txt: {e}")
-- MAGIC         print("   (Verifique se o arquivo foi subido com esse nome exato na pasta gtfs_upload)")
-- MAGIC
-- MAGIC print("\n🏁 Processo finalizado!")

-- COMMAND ----------

-- DBTITLE 1,Lista arquivos salvos no storage do Azure
-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("abfss://datalakeprojmba@datalakeprojmba.dfs.core.windows.net/olhovivo/bronze/buscar_linhas"));
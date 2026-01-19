# Criação das tabelas GTFS na camada bronze


# ============================================================
# Ingestão de Dados Estáticos (GTFS)
# Camada: Bronze (Azure Standard)
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, input_file_name
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# --- 1. CONFIGURAÇÕES ---
storage_account = "datalakeprojmba"
container = "datalakeprojmba"
base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

# Pasta onde você fez o upload no Azure
caminho_origem = f"{base_path}/olhovivo/bronze/gtfs_upload"

# Lista dos arquivos padrão do GTFS
# O script vai procurar por esses nomes exatos na pasta
arquivos_gtfs = [
     "agency",
     "calendar",
     "fare_attributes",
     "fare_rules",
     "frequencies", 
     "routes",
     "shapes",
     "stop_times",
     "stops",
     "trips"
]

timestamp_ingestao = datetime.now()

# --- 2. PROCESSAMENTO ---
print(f" Buscando arquivos em: {caminho_origem}")

for arquivo in arquivos_gtfs:
    print(f"\n Processando {arquivo}...")
     
    # Monta o caminho: .../gtfs_upload/stops.txt
    path_file = f"{caminho_origem}/{arquivo}.txt"
     
    try:
         # Lê o TXT como CSV (o GTFS é separado por vírgula)
         df = (spark.read
               .format("csv")
               .option("header", "true")
               .option("inferSchema", "true") # Pode inferir tipos para agilizar na Bronze
               .option("delimiter", ",")
               .load(path_file))
         
         # Adiciona metadados
         df = df.withColumn("dt_ingestao", lit(timestamp_ingestao)) \
                .withColumn("arquivo_origem", input_file_name())

         # Define nome da tabela: olhovivo_bronze.gtfs_stops
         nome_tabela = f"olhovivo_bronze.gtfs_{arquivo}"
         
         # Grava como tabela Delta (Overwrite, pois GTFS é cadastro mestre)
         df.write \
           .format("delta") \
           .mode("overwrite") \
           .option("overwriteSchema", "true") \
           .saveAsTable(nome_tabela)
           
         print(f" Tabela criada com sucesso: {nome_tabela} ({df.count()} linhas)")
         
    except Exception as e:

         print(f" Erro ao ler {arquivo}.txt: {e}")
         print(" (Verifique se o arquivo foi subido com esse nome exato na pasta gtfs_upload)")

print("\n Processo finalizado!")

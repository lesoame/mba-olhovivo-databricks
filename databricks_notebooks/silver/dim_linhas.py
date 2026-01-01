# Databricks notebook source
# ============================================================
# Camada Silver: Dimensão Linhas
# Cruzamento: API (Códigos) + GTFS (Nomes de rotas)
# ============================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, row_number, trim, when
from pyspark.sql.window import Window

# 1. Ler tabelas
df_api = spark.table("olhovivo_bronze.buscar_linhas")
df_gtfs_routes = spark.table("olhovivo_bronze.gtfs_routes")

# 2. Tratamento API: Deduplicar e criar chave de ligação
w_api = Window.partitionBy("cl").orderBy(col("dt_ingestao").desc())

df_api_clean = df_api.withColumn("rn", row_number().over(w_api)) \
    .filter(col("rn") == 1) \
    .select(
        col("cl").alias("cod_linha"),
        col("lc").alias("is_circular"),
        # Concatena para bater com o GTFS (Ex: 1012 + - + 10 = 1012-10)
        concat(col("lt"), lit("-"), col("tl")).alias("letreiro_completo"),
        col("tp").alias("nome_api_tp_ts"),
        col("ts").alias("nome_api_ts_tp"),
        col("sl").alias("sentido_api")
    )

# 3. Tratamento GTFS: Pegar nomes oficiais
# Removemos duplicatas de letreiro no GTFS para evitar explosão no Join
df_gtfs_clean = df_gtfs_routes.select(
    col("route_short_name").alias("letreiro_gtfs"),
    col("route_long_name").alias("nome_rota_oficial") # Ex: Term. Jd. Britania - Jd. Monte Belo
).dropDuplicates(["letreiro_gtfs"])

# 4. Join Final
df_silver = df_api_clean.join(
    df_gtfs_clean,
    df_api_clean.letreiro_completo == df_gtfs_clean.letreiro_gtfs,
    "left"
)

# 5. Gravação
df_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("olhovivo_silver.dim_linhas")

print("✅ Dimensão Linhas criada com sucesso!")
display(df_silver.limit(5))
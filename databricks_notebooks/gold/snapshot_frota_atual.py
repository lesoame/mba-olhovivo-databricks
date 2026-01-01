# Databricks notebook source
# ============================================================
# Camada Gold: Snapshot Atual (Para Mapas em Tempo Real)
# ============================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

df_posicao = spark.table("olhovivo_silver.fato_posicao_veiculos")

# Pega apenas o ÚLTIMO registro de cada veículo no banco todo
w_last = Window.partitionBy("prefixo_veiculo").orderBy(col("data_hora_sp").desc())

df_snapshot = df_posicao.withColumn("rn", row_number().over(w_last)) \
    .filter(col("rn") == 1) \
    .drop("rn")

df_snapshot.write.format("delta").mode("overwrite").saveAsTable("olhovivo_gold.snapshot_frota_atual")

print("✅ Tabela de Mapa em Tempo Real atualizada!")

df_snapshot.limit(10).display()
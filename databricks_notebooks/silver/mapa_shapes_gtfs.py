# Databricks notebook source
# ============================================================
# Camada Silver: Shapes (Geometria das Rotas)
# Objetivo: Preparar dados para plotar linhas no mapa
# ============================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

df_shapes = spark.table("olhovivo_bronze.gtfs_shapes")
df_trips = spark.table("olhovivo_bronze.gtfs_trips")

# 1. Selecionamos Trips únicas por rota e shape
# Queremos saber: "Para a linha 1012-10, qual é o ID do desenho (shape)?"
df_trips_unique = df_trips.select("route_id", "shape_id", "trip_headsign").dropDuplicates(["route_id", "shape_id"])

# 2. Join com os pontos de latitude/longitude
df_silver_shapes = df_shapes.join(df_trips_unique, "shape_id", "inner") \
    .select(
        col("route_id").alias("letreiro_completo"), # Chave para ligar com a linha
        col("shape_id"),
        col("shape_pt_lat").alias("latitude"),
        col("shape_pt_lon").alias("longitude"),
        col("shape_pt_sequence").alias("sequencia"), # Importante para ordenar a linha no mapa
        col("trip_headsign").alias("destino_escrito")
    )

df_silver_shapes.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("olhovivo_silver.mapa_shapes_gtfs")

print("✅ Tabela de Mapa (Shapes) criada!")
display(df_silver_shapes.limit(5))